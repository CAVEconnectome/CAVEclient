import contextlib
import os
import webbrowser

import caveclient


class AccountSetup:
    def __init__(self, datastack_name, server_address):
        self._datastack_name = datastack_name
        self._server_address = server_address

    def setup_new_token(self):
        # Get token if not already found
        cc_global = caveclient.CAVEclient(server_address=self._server_address)
        if cc_global.auth.token is None:
            print("Taking you to the CAVE site to get a token")
            # supress print output until an actual caveclient option is merged
            with open(os.devnull, "w") as f, contextlib.redirect_stdout(f):
                cc_global.auth.get_new_token(open=True)
            new_token = input(
                "Paste your token in here and then check terms of service"
            )
            cc_global.auth.save_token(new_token)
            print(f"Saved token for {self._server_address}")
        else:
            print(f"Token for {self._server_address} found!")

        # try to check the datastack and fall back to ToS if not found
        try:
            client = caveclient.CAVEclient(
                datastack_name=self._datastack_name,
                server_address=self._server_address,
            )
            client.info.get_datastack_info()
        except:
            print("Please click through dataset terms of service")
            info_url = f"{self._server_address}/info/datastack/{self._datastack_name}"
            webbrowser.open(info_url)

        # Validate one last time
        try:
            client = caveclient.CAVEclient(
                datastack_name=self._datastack_name,
                server_address=self._server_address,
            )
            client.info.get_datastack_info()
            print(f"Success! You can now work on {self._datastack_name}")
        except Exception as e:
            print(f"Token setup failed. Check values and permissions:\n {e}")
        pass
