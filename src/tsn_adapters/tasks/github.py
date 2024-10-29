import io
import pandas as pd
from prefect import task
from pydantic import SecretStr

from github import Github, GithubException


@task
def task_read_repo_csv_file(repo: str, path: str, branch: str = "main", gh_token: SecretStr = None) -> pd.DataFrame:
    return read_repo_csv_file(repo, path, branch, gh_token)

def read_repo_csv_file(repo: str, path: str, branch: str = "main", gh_token: SecretStr = None) -> pd.DataFrame:
    g = Github(gh_token.get_secret_value())
    try:
        repository_api = g.get_repo(repo)
    except GithubException as e:
        if e.status == 404:
            if not gh_token:
                raise Exception(
                    "Repository not found (404). It might be private or a GitHub token is missing."
                )
            else:
                raise Exception(
                    "Repository not found (404). Please ensure your GitHub token has read access to the repository."
                )
        else:
            raise e
    file_content = repository_api.get_contents(path, ref=branch)
    df = pd.read_csv(io.StringIO(file_content.decoded_content.decode()))
    return df


