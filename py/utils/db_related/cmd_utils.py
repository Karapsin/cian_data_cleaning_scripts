import subprocess
import shlex

def run_cmd(input_str):
    subprocess.run(shlex.split(input_str), check = True)

def start_db():
    run_cmd("docker start mongo")

def stop_db():
    run_cmd("docker stop mongo")

def rm_db():
    run_cmd("docker rm mongo")

def delete_db():
    run_cmd("rm -rf db")

def run_sh(path):
    result = subprocess.run(["bash", path], capture_output=True, text=True)
    print("STDOUT:", result.stdout)
    print("STDERR:", result.stderr)