from prefect import task, flow
import time

@task
def step_1_say_hello():
    print("👋 Hello from Prefect! Я - тестовый шаг 1.")
    time.sleep(1)

@task
def step_2_do_work():
    print("⚙️ Я - тестовый шаг 2. Что-то делаю...")
    time.sleep(2)

@task
def step_3_say_goodbye():
    print("✅ Работа выполнена. Goodbye!")

@flow(name="Test Hello World Flow", log_prints=True)
def hello_world_flow():
    step_1_say_hello()
    step_2_do_work()
    step_3_say_goodbye()

if __name__ == "__main__":
    hello_world_flow.to_deployment(name="hello-world-deployment", work_pool_name="default-agent-pool")