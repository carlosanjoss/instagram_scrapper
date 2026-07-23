import argparse
import csv
import time
from pathlib import Path
from threading import Event, Thread
from typing import List

from dotenv import find_dotenv, load_dotenv

from src.auth.autenticacao import login_with_persistence
from src.services.orchestrator.orchestrator import InstagramKafkaOrchestrator
from src.services.kafka.producer import KafkaService
from src.services.orchestrator.task_contract import create_fetch_user_task


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

        has_header = False
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
        description="Inicia pipeline completo de coleta Instagram via Kafka"
    )
    parser.add_argument(
        "--csv",
        dest="csv_path",
        default="users.csv",
        help="Caminho do CSV com lista de usuários (padrão: users.csv)",
    )
    parser.add_argument("--media-limit", type=int, default=10)
    parser.add_argument("--stories-limit", type=int, default=10)
    parser.add_argument("--comments-limit", type=int, default=30)
    parser.add_argument("--task-topic", default="instagram.tasks")
    parser.add_argument("--user-state-topic", default="instagram.user.latest")
    parser.add_argument("--media-comments-state-topic", default="instagram.media.comments.latest")
    parser.add_argument("--user-stories-state-topic", default="instagram.user.stories.latest")
    parser.add_argument("--processed-task-topic", default="instagram.tasks.processed")
    parser.add_argument("--user-observation-topic", default="instagram.user.observations")
    parser.add_argument("--media-observation-topic", default="instagram.media.observations")
    parser.add_argument("--bootstrap-server", default="localhost:29092")
    parser.add_argument("--group-id", default="instagram-orchestrator-workers")
    parser.add_argument("--delay-min", type=float, default=8.0)
    parser.add_argument("--delay-max", type=float, default=40.0)
    parser.add_argument("--cycle-interval", type=int, default=3600, help="Intervalo em segundos entre ciclos de enfileiramento (padrão: 3600s = 1h)")
    return parser.parse_args()


def enqueue_tasks(
    usernames: List[str],
    media_limit: int,
    stories_limit: int,
    comments_limit: int,
    task_topic: str,
    bootstrap_server: str,
) -> None:
    kafka = KafkaService(bootstrap_servers=bootstrap_server)
    for username in usernames:
        task = create_fetch_user_task(
            username,
            media_limit=media_limit,
            stories_limit=stories_limit,
            comments_limit=comments_limit,
        )
        kafka.send_to_topic(topic=task_topic, key=username, data=task)
        print(f"Tarefa enfileirada para @{username}")


def run_pipeline(args: argparse.Namespace, usernames: List[str]) -> None:
    client = login_with_persistence()
    orchestrator = InstagramKafkaOrchestrator(
        client=client,
        task_topic=args.task_topic,
        user_state_topic=args.user_state_topic,
        media_comments_state_topic=args.media_comments_state_topic,
        user_stories_state_topic=args.user_stories_state_topic,
        processed_task_topic=args.processed_task_topic,
        user_observation_topic=args.user_observation_topic,
        media_observation_topic=args.media_observation_topic,
        bootstrap_servers=args.bootstrap_server,
        consumer_group=args.group_id,
        min_delay_seconds=args.delay_min,
        max_delay_seconds=args.delay_max,
    )

    ready_event = Event()
    consumer_thread = Thread(target=orchestrator.run_consumer, kwargs={"ready_event": ready_event}, daemon=True)
    consumer_thread.start()
    ready_event.wait(timeout=15)

    cycle = 0
    while True:
        cycle += 1
        print(f"\nCiclo #{cycle} - Enfileirando {len(usernames)} usuários...")
        enqueue_tasks(
            usernames=usernames,
            media_limit=args.media_limit,
            stories_limit=args.stories_limit,
            comments_limit=args.comments_limit,
            task_topic=args.task_topic,
            bootstrap_server=args.bootstrap_server,
        )
        print(f"Total de tarefas enviadas para {args.task_topic}: {len(usernames)}")
        print(f"Próximo ciclo em {args.cycle_interval}s...")
        time.sleep(args.cycle_interval)

if __name__ == "__main__":
    load_dotenv(find_dotenv())
    args = parse_args()

    usernames = load_usernames_from_csv(args.csv_path)
    if not usernames:
        raise ValueError("Nenhum usuário encontrado no arquivo CSV")

    run_pipeline(args=args, usernames=usernames)
