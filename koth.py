import requests
import time
import chess
import chess.pgn
import chess.polyglot
import chess.variant

BOTS = ["NimsiluBot", "VariantsBot"]
VARIANT = "kingOfTheHill"   # change to atomic, horde, etc
MIN_ELO = 2550
MAX_PLY = 60
MAX_GAMES = 10000   # safety cap
SLEEP_BETWEEN_REQUESTS = 1.0

PGN_OUTPUT = f"{VARIANT}_games.pgn"
BOOK_OUTPUT = f"{VARIANT}_book.bin"


def fetch_games(username, variant, min_elo, max_games):
    url = f"https://lichess.org/api/games/user/{username}"
    params = {
        "max": max_games,
        "perfType": variant,
        "clocks": False,
        "evals": False,
        "moves": True,
        "pgnInJson": True,
        "rated": True
    }
    headers = {"Accept": "application/x-ndjson"}

    games = []
    with requests.get(url, params=params, headers=headers, stream=True, timeout=60) as r:
        for line in r.iter_lines():
            if not line:
                continue
            game = line.decode("utf-8")
            games.append(game)
    return games


def save_pgn(games, output_file):
    with open(output_file, "w", encoding="utf-8") as f:
        for g in games:
            f.write(g + "\n\n")


def build_bin_from_pgn(pgn_file, bin_file):
    with open(pgn_file, "r", encoding="utf-8") as f:
        game = chess.pgn.read_game(f)
        with chess.polyglot.Writer(open(bin_file, "wb")) as writer:
            while game:
                board = game.board()
                for move in game.mainline_moves():
                    if board.fullmove_number <= MAX_PLY:
                        entry = chess.polyglot.Entry.from_board(
                            board, move, weight=1, learn=0
                        )
                        writer.write(entry)
                    board.push(move)
                game = chess.pgn.read_game(f)


if __name__ == "__main__":
    all_games = []
    for bot in BOTS:
        print(f"Fetching games for {bot}...")
        games = fetch_games(bot, VARIANT, MIN_ELO, MAX_GAMES)
        print(f"Fetched {len(games)} games from {bot}")
        all_games.extend(games)
        time.sleep(SLEEP_BETWEEN_REQUESTS)

    print(f"Saving {len(all_games)} total games to {PGN_OUTPUT}")
    save_pgn(all_games, PGN_OUTPUT)

    print("Building .bin book...")
    build_bin_from_pgn(PGN_OUTPUT, BOOK_OUTPUT)
    print(f"Book saved as {BOOK_OUTPUT}")
