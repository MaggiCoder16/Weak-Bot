import requests
import json
import time
import io
import random
import chess
import chess.pgn
import chess.polyglot
import chess.variant

BOTS = ["NimsiluBot", "MaggiChess16", "VariantsBot"]

VARIANT = "horde"
MIN_ELO = 2530
CHUNK_SIZE = 5000
REQUEST_TIMEOUT = 120
SLEEP_BETWEEN_CHUNKS = 0.4
MAX_PLY = 60
MAX_BOOK_WEIGHT = 2520

PGN_OUTPUT = f"{VARIANT}_games.pgn"
BOOK_OUTPUT = f"{VARIANT}_book.bin"


def fetch_all_games_for_bot(bot: str) -> list[str]:
    print(f"Fetching {VARIANT} games for {bot} (rating >= {MIN_ELO})...")
    base_url = f"https://lichess.org/api/games/user/{bot}"
    headers = {"Accept": "application/x-ndjson"}
    params = {
        "max": CHUNK_SIZE,
        "perfType": VARIANT,
        "rated": "true",
        "moves": "true",
        "pgnInJson": "true",
        "clocks": "false",
        "evals": "false",
        "opening": "false",
    }

    all_pgns: list[str] = []
    until_ts: int | None = None
    total_lines = 0
    kept = 0
    seen_ids: set[str] = set()

    while True:
        if until_ts is not None:
            params["until"] = until_ts
        else:
            params.pop("until", None)

        resp = requests.get(
            base_url, params=params, headers=headers, stream=True, timeout=REQUEST_TIMEOUT
        )
        resp.raise_for_status()

        batch_count = 0
        earliest_ts = None

        for raw in resp.iter_lines():
            if not raw:
                continue
            batch_count += 1
            total_lines += 1

            try:
                game = json.loads(raw.decode("utf-8"))
            except json.JSONDecodeError:
                continue

            gid = str(game.get("id") or "")
            if gid in seen_ids:
                continue
            if gid:
                seen_ids.add(gid)

            created_at = game.get("createdAt")
            if isinstance(created_at, int):
                if earliest_ts is None or created_at < earliest_ts:
                    earliest_ts = created_at

            try:
                w = game["players"]["white"]
                b = game["players"]["black"]
                white_rating = int(w.get("rating", 0) or 0)
                black_rating = int(b.get("rating", 0) or 0)
            except Exception:
                continue

            if max(white_rating, black_rating) < MIN_ELO:
                continue

            variant = (game.get("variant") or "").lower()
            if variant and variant != VARIANT:
                continue

            pgn = game.get("pgn")
            if pgn:
                all_pgns.append(pgn)
                kept += 1

        print(f"  chunk: got {batch_count} games, kept {kept} total so far")

        if batch_count == 0 or earliest_ts is None:
            break

        until_ts = earliest_ts - 1
        time.sleep(SLEEP_BETWEEN_CHUNKS)

    print(f"Finished {bot}: processed {total_lines} lines, kept {kept} games ≥ {MIN_ELO}")
    return all_pgns


def save_merged_pgn(pgn_list: list[str], out_path: str) -> None:
    print("Merging PGNs...")
    with open(out_path, "w", encoding="utf-8") as f:
        for p in pgn_list:
            f.write(p)
            if not p.endswith("\n"):
                f.write("\n")
            f.write("\n")
    print(f"Saved merged PGN to {out_path} ({len(pgn_list)} games)")


class BookMove:
    def __init__(self):
        self.weight = 0
        self.move: chess.Move | None = None


class BookPosition:
    def __init__(self):
        self.moves: dict[str, BookMove] = {}

    def get_move(self, uci: str) -> BookMove:
        return self.moves.setdefault(uci, BookMove())


class Book:
    def __init__(self):
        self.positions: dict[str, BookPosition] = {}

    def get_position(self, key_hex: str) -> BookPosition:
        return self.positions.setdefault(key_hex, BookPosition())

    def normalize(self):
        for pos in self.positions.values():
            s = sum(bm.weight for bm in pos.moves.values())
            if s <= 0:
                continue
            for bm in pos.moves.values():
                # scale to MAX_BOOK_WEIGHT, keep minimum 1
                bm.weight = max(1, int(bm.weight / s * MAX_BOOK_WEIGHT))

    def save_polyglot(self, path: str):
        entries = []
        for key_hex, pos in self.positions.items():
            zbytes = bytes.fromhex(key_hex)
            for uci, bm in pos.moves.items():
                if bm.weight <= 0 or bm.move is None:
                    continue
                m = bm.move
                # skip drop-style moves (not relevant for Horde but safe)
                if "@" in m.uci():
                    continue
                mi = m.to_square + (m.from_square << 6)
                if m.promotion:
                    mi += ((m.promotion - 1) << 12)
                mbytes = mi.to_bytes(2, "big")
                wbytes = min(MAX_BOOK_WEIGHT, bm.weight).to_bytes(2, "big")
                lbytes = (0).to_bytes(4, "big")
                entries.append(zbytes + mbytes + wbytes + lbytes)
        # sort by Zobrist then by move bytes to get deterministic file
        entries.sort(key=lambda e: (e[:8], e[10:12]))
        with open(path, "wb") as f:
            for e in entries:
                f.write(e)
        print(f"Saved {len(entries)} moves to book: {path}")


def key_hex(board: chess.Board) -> str:
    return f"{chess.polyglot.zobrist_hash(board):016x}"


def build_book_from_pgn(pgn_path: str, bin_path: str):
    print("Building book...")
    book = Book()
    with open(pgn_path, "r", encoding="utf-8") as f:
        data = f.read()
    stream = io.StringIO(data)

    processed = 0
    while True:
        game = chess.pgn.read_game(stream)
        if game is None:
            break
        if (game.headers.get("Variant", "") or "").lower() != VARIANT:
            continue

        # Ignore very short/low-signal games
        try:
            last_node = game.end()
            if last_node.board().fullmove_number < 10:
                continue
        except Exception:
            pass

        board = chess.variant.HordeBoard()
        result = game.headers.get("Result", "*")

        for ply, move in enumerate(game.mainline_moves()):
            if ply >= MAX_PLY:
                break

            k = key_hex(board)
            pos = book.get_position(k)
            bm = pos.get_move(move.uci())
            bm.move = move

            # Emphasize early moves (opening importance)
            decay = max(1, (MAX_PLY - ply) // 5)

            # Strong bias toward moves played by the eventual winner
            if result == "1-0":
                # If it's White to move (White played this move), reward heavily; small reward for Black's replies
                bm.weight += (6 if board.turn == chess.WHITE else 1) * decay
            elif result == "0-1":
                bm.weight += (6 if board.turn == chess.BLACK else 1) * decay
            elif result == "1/2-1/2":
                bm.weight += 2 * decay

            board.push(move)

        processed += 1
        if processed % 100 == 0:
            print(f"Processed {processed} games")

    book.normalize()
    # Optional gentle variety to break ties (keeps strength but avoids lock-in)
    for pos in book.positions.values():
        for bm in pos.moves.values():
            bm.weight = min(MAX_BOOK_WEIGHT, bm.weight + random.randint(0, 3))

    book.save_polyglot(bin_path)


def main():
    all_pgns: list[str] = []
    for bot in BOTS:
        all_pgns.extend(fetch_all_games_for_bot(bot))
    save_merged_pgn(all_pgns, PGN_OUTPUT)
    build_book_from_pgn(PGN_OUTPUT, BOOK_OUTPUT)
    print("Done.")


if __name__ == "__main__":
    main()
