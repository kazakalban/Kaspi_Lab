from airflow.sdk import dag, task

@dag(
        dag_id="greeting_students",
        description="A simple DAG to greet students and a teacher",
        schedule=None

)
def my_first_dag():

    @task
    def greeting_students() -> None:
        """Prints a gretting message to students."""
        students: list[str] = ["Alice", "Bob", "Charlie"]

        student: str
        for student in students:
            print(f"Hello, {student}! Welcome to the course.")
        
    @task
    def greting_teacher() -> None:
        """Prints a greeting mesage to teacher."""
        print("Hello! Teacher Kassym!")


    @task
    def hello_word() -> None:
        print("Hello world!")

    hello_word()
    greeting_students()
    greting_teacher()
my_first_dag()