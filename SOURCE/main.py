import argparse
import csv
from pathlib import Path
from typing import List

from dotenv import find_dotenv, load_dotenv

from src.auth.autenticacao import login_with_persistence
from src.services.collector import InstagramJsonCollector
from src.services.storage import JsonRepository
from src.utils.retry import RetryPolicy


def load_usernames_from_csv(csv_path: str) -> List[str]:
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"CSV não encontrado: {csv_path}")

    usernames: List[str] = []
    with path.open("r", encoding="utf-8-sig", newline="") as file_obj:
        sample = file_obj.read(2048)
        file_obj.seek(0)

        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",;")
        except csv.Error:
            dialect = csv.excel

        try:
            has_header = csv.Sniffer().has_header(sample)
        except csv.Error:
            has_header = False

        if has_header:
            reader = csv.DictReader(file_obj, dialect=dialect)
            if reader.fieldnames and "username" in reader.fieldnames:
                for row in reader:
                    value = (row.get("username") or "").strip()
                    if value:
                        usernames.append(value)
                return usernames

        file_obj.seek(0)
        raw_reader = csv.reader(file_obj, dialect=dialect)
        for row in raw_reader:
            if row and row[0].strip() and row[0].strip().lower() != "username":
                usernames.append(row[0].strip())
    return usernames


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Coleta perfis do Instagram e salva os resultados em JSON"
    )
    parser.add_argument(
        "--csv",
        dest="csv_path",
        default="users.csv",
        help="CSV com um username por linha (padrão: users.csv)",
    )
    parser.add_argument(
        "--output",
        default="data",
        help="Diretório dos arquivos JSON (padrão: data)",
    )
    parser.add_argument("--media-limit", type=int, default=10)
    parser.add_argument("--stories-limit", type=int, default=10)
    parser.add_argument("--comments-limit", type=int, default=30)
    parser.add_argument("--delay-min", type=float, default=8.0)
    parser.add_argument("--delay-max", type=float, default=40.0)
    parser.add_argument(
        "--cycle-interval",
        type=int,
        default=3600,
        help="Intervalo entre ciclos em segundos (padrão: 3600)",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Executa somente um ciclo de coleta",
    )
    args = parser.parse_args()
    for name in ("media_limit", "stories_limit", "comments_limit"):
        if getattr(args, name) < 0:
            parser.error(f"--{name.replace('_', '-')} não pode ser negativo")
    if args.cycle_interval <= 0:
        parser.error("--cycle-interval deve ser positivo")
    if args.delay_min < 0 or args.delay_max < args.delay_min:
        parser.error("o intervalo entre --delay-min e --delay-max é inválido")
    return args


def run_pipeline(args: argparse.Namespace, usernames: List[str]) -> None:
    client = login_with_persistence()
    repository = JsonRepository(args.output)
    collector = InstagramJsonCollector(
        client=client,
        repository=repository,
        min_delay_seconds=args.delay_min,
        max_delay_seconds=args.delay_max,
    )

    cycle = 0
    try:
        while True:
            cycle += 1
            print(f"\nCiclo #{cycle} - coletando {len(usernames)} usuários")
            for username in usernames:
                try:
                    collector.collect_user(
                        username,
                        media_limit=args.media_limit,
                        stories_limit=args.stories_limit,
                        comments_limit=args.comments_limit,
                    )
                except Exception as exc:
                    if RetryPolicy.is_fatal(exc):
                        raise
                    print(f"Falha ao coletar @{username}; seguindo para o próximo: {exc}")
            print(f"Dados JSON salvos em: {repository.output_dir.resolve()}")
            if args.once:
                return
            print(f"Próximo ciclo em {args.cycle_interval}s...")
            collector.sleep(args.cycle_interval)
    except KeyboardInterrupt:
        print("\nColeta encerrada pelo usuário.")


if __name__ == "__main__":
    load_dotenv(find_dotenv())
    arguments = parse_args()
    targets = load_usernames_from_csv(arguments.csv_path)
    if not targets:
        raise ValueError("Nenhum usuário encontrado no arquivo CSV")
    run_pipeline(args=arguments, usernames=targets)
