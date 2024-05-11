import click
from .tools.onboarding import AccountSetup


@click.group()
def cli():
    "Tools for setting up a computer to use caveclient"
    pass


@cli.command()
@click.option(
    "--datastack_name",
    "-d",
    prompt="Datastack Name",
    help="Name of the datastack to set up",
)
@click.option(
    "--server_address",
    "-s",
    help="Address of the server to set up token for",
    prompt="Server Address",
    default="https://global.daf-apis.com",
)
def setup(datastack_name, server_address):
    onboard = AccountSetup(datastack_name, server_address)
    msg = f"""Setting up token for datastack {datastack_name} on server {server_address}.
    This might open one or more browser windows.
    Press Enter to continue."""
    input(msg)
    onboard.setup_new_token()


if __name__ == "__main__":
    cli()
