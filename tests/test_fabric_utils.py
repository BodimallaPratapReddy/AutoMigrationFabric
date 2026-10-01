import unittest
from collections.abc import Iterator
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import Mock, patch

import httpx

from thirdparty.fabric import utils
from thirdparty.fabric.exceptions import FabricAuthenticationError


class ConnectToFabricTests(unittest.TestCase):
    @patch.object(utils, "load_dotenv")
    @patch.object(utils.os, "getenv", side_effect={
        "FABRIC_TENANT_ID": "tenant", "FABRIC_CLIENT_ID": "client", "FABRIC_CLIENT_SECRET": "secret"}.get)
    @patch.object(utils, "ClientSecretCredential")
    def test_token_failure_is_normalized_and_secret_safe(self, credential_class: Mock, _: Mock, __: Mock) -> None:
        credential = credential_class.return_value
        credential.get_token.side_effect = RuntimeError("secret token acquisition failed")
        with self.assertRaises(FabricAuthenticationError) as raised:
            with utils.connect_to_fabric():
                pass
        self.assertNotIn("secret", str(raised.exception))
        credential.close.assert_called_once()

    def test_token_refresh_failure_is_normalized(self) -> None:
        credential = Mock()
        credential.get_token.side_effect = RuntimeError("secret refresh failed")
        request = httpx.Request("GET", utils.FABRIC_API_URL)
        with self.assertRaises(FabricAuthenticationError) as raised:
            list(utils._ServicePrincipalAuth(credential).auth_flow(request))
        self.assertNotIn("secret", str(raised.exception))

    @patch.object(utils, "load_dotenv")
    @patch.object(utils.os, "getenv")
    @patch.object(utils, "ClientSecretCredential")
    def test_authenticated_request(self, credential_class: Mock, getenv: Mock, _: Mock) -> None:
        config = {
            "FABRIC_TENANT_ID": "tenant",
            "FABRIC_CLIENT_ID": "client",
            "FABRIC_CLIENT_SECRET": "secret",
        }
        getenv.side_effect = config.get
        credential = credential_class.return_value
        credential.get_token.side_effect = [
            SimpleNamespace(token="initial-token"),
            SimpleNamespace(token="request-token"),
        ]

        def respond(request: httpx.Request) -> httpx.Response:
            self.assertEqual(str(request.url), f"{utils.FABRIC_API_URL}workspaces")
            self.assertEqual(request.headers["Authorization"], "Bearer request-token")
            return httpx.Response(200, json={"value": []})

        real_client = httpx.Client
        transport = httpx.MockTransport(respond)
        with patch.object(
            utils.httpx,
            "Client",
            side_effect=lambda **kwargs: real_client(transport=transport, **kwargs),
        ):
            with utils.connect_to_fabric() as client:
                self.assertEqual(client.get("workspaces").json(), {"value": []})

        credential_class.assert_called_once_with(
            tenant_id="tenant", client_id="client", client_secret="secret"
        )
        self.assertEqual(credential.get_token.call_count, 2)
        credential.get_token.assert_called_with(utils.FABRIC_SCOPE)
        credential.close.assert_called_once()

    @patch.object(utils, "load_dotenv")
    @patch.object(utils.os, "getenv", return_value=None)
    def test_missing_configuration(self, _: Mock, __: Mock) -> None:
        with self.assertRaisesRegex(ValueError, "FABRIC_TENANT_ID"):
            with utils.connect_to_fabric():
                pass

    def test_list_lakehouses_across_pages(self) -> None:
        workspace_id = "cfafbeb1-8037-4d0c-896e-a46fb27ff229"
        requests: list[httpx.Request] = []

        def respond(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            if len(requests) == 1:
                return httpx.Response(
                    200,
                    json={
                        "value": [{"id": "lakehouse-1", "displayName": "Bronze"}],
                        "continuationToken": "next-page",
                    },
                )
            return httpx.Response(
                200, json={"value": [{"id": "lakehouse-2", "displayName": "Silver"}]}
            )

        @contextmanager
        def client_context() -> Iterator[httpx.Client]:
            with httpx.Client(
                base_url=utils.FABRIC_API_URL,
                transport=httpx.MockTransport(respond),
            ) as client:
                yield client

        with patch.object(utils, "connect_to_fabric", side_effect=client_context):
            result = utils.list_lakehouses(workspace_id)

        self.assertEqual(
            result,
            [{"id": "lakehouse-1", "name": "Bronze"}, {"id": "lakehouse-2", "name": "Silver"}],
        )
        self.assertEqual(
            str(requests[0].url),
            f"{utils.FABRIC_API_URL}workspaces/{workspace_id}/lakehouses",
        )
        self.assertEqual(requests[1].url.params["continuationToken"], "next-page")

    def test_list_lakehouses_rejects_invalid_workspace_id(self) -> None:
        with self.assertRaisesRegex(ValueError, "workspace_id must be a valid UUID"):
            utils.list_lakehouses("invalid")


if __name__ == "__main__":
    unittest.main()
