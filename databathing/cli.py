import json
import os
import re
import sys
import pwd


from pathlib import Path
from typing import List, Optional

import inspect
import typer

from databathing import ERRORS, __app_name__, __version__

from databathing.querybathing import querybathing
from databathing.pipebathing import pipe_run, init_spark, yaml_storage
from databathing.pipebathing import yaml_storage

app = typer.Typer()


@app.command()
def ui(
    # uri,
    # entry_point,
    # version,
    # param_list,
    # docker_args,
    # experiment_name,
    # experiment_id,
    # backend,
    # backend_config,
    # env_manager,
    # storage_dir,
    # run_id,
    # run_name,
    # build_image,
):
    """
    Run the databathing pipeline..
    """
    typer.secho(
        "UI should be started up!",
        fg=typer.colors.GREEN
    )

@app.command()
def qryparse(
    query: str = typer.Option(
        ...,
        "--query",
        "-q",
        prompt="query",
    )
):
    """
    Run the databathing pipeline..
    """
    typer.secho(
        querybathing(query).parse(),
        fg=typer.colors.GREEN
    )

@app.command()
def piperun(
    extracts_yaml: str = typer.Option(
        ...,
        "--extracts_yaml",
        "-ey",
        prompt="Pls pass extracts_yaml"
    ),
    transforms_yaml: str = typer.Option(
        ..., 
        "--transforms_yaml",
        "-ty",
        prompt="Pls pass transforms_yaml"
    ),
    loaders_yaml: str = typer.Option(
        ...,
        "--loaders_yaml",
        "-ly",
        prompt="Pls pass loaders_yaml"
    ),
):
    """
    Run the databathing pipeline..
    """
    user = pwd.getpwuid(os.getuid())[0]
    project_name = "demo"
    init_spark(project_name, user)

    yaml_storage["extracts"] = extracts_yaml
    yaml_storage["transforms"] = transforms_yaml
    yaml_storage["loaders"] = loaders_yaml

    pipe_run()
    

def _version_callback(value: bool) -> None:
    if value:
        typer.echo(f"{__app_name__} v{__version__}")
        raise typer.Exit()


@app.callback()
def main(
    version: Optional[bool] = typer.Option(
        None,
        "--version",
        "-v",
        help="Show the application's version and exit.",
        callback=_version_callback,
        is_eager=True,
    )
) -> None:
    return