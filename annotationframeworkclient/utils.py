class AuthException(Exception):
    pass


def handle_response(response, as_json=True):
    '''Deal with endpoint response'''
    response.raise_for_status()
    check_authorization_redirect(response)
    if as_json:
        return response.json()
    else:
        return response


def check_authorization_redirect(response):
    if len(response.history) == 0:
        pass
    else:
        raise AuthException(
            f"""You do not have permission to use the endpoint {response.history[0].url} with the current auth configuration.\nRead the documentation or follow instructions under client.auth.get_new_token() for how to set a valid API token.""")
