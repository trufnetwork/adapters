import io

import pandas as pd
from prefect import Task, task
from prefect.blocks.core import Block
from pydantic import ConfigDict, SecretStr


class GithubAccess(Block):
    """Prefect Block for managing GitHub access credentials.

    This block securely stores and manages GitHub access tokens for
    authenticating with GitHub API in Prefect flows.
    """

    github_token: SecretStr
    model_config = ConfigDict(ignored_types=(Task,))

    def read_repo_csv_file(self, repo: str, path: str, branch: str = "main") -> pd.DataFrame:
        # can import only here as it's not available in the server
        from github import Github, GithubException

        g = Github(self.github_token.get_secret_value())
        try:
            repository_api = g.get_repo(repo)
        except GithubException as e:
            if e.status == 404:
                if not self.github_token:
                    raise Exception(
                        "Repository not found (404). It might be private or a GitHub token is missing."
                    ) from e
                else:
                    raise Exception(
                        "Repository not found (404). Please ensure your GitHub token has read access to the repository."
                    ) from e
            else:
                raise e

        file_content = repository_api.get_contents(path, ref=branch)
        if isinstance(file_content, list):
            file_content = file_content[0]
        df: pd.DataFrame = pd.read_csv(io.StringIO(file_content.decoded_content.decode()), dtype=str)
        return df


# --- Top Level Task Functions ---
@task
def read_repo_csv_file(block: GithubAccess, repo: str, path: str, branch: str = "main") -> pd.DataFrame:
    return block.read_repo_csv_file(repo=repo, path=path, branch=branch)


if __name__ == "__main__":
    GithubAccess.register_type_and_schema()
