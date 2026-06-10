from unittest.mock import MagicMock, patch

import frappe
from frappe.tests import IntegrationTestCase, change_settings

from frappe_gmail_thread.api.oauth import (
    authorize_access,
    callback,
    check_gmail_object,
    get_access_token,
    get_auth_url,
)
from frappe_gmail_thread.tests import (
    TEST_USER,
    TEST_USER_2,
    as_user,
    make_test_gmail_account,
    make_test_user,
)

OAUTH_MODULE = "frappe_gmail_thread.api.oauth"


def _ensure_account():
    make_test_user(TEST_USER)
    with as_user(TEST_USER):
        return make_test_gmail_account(linked_user=TEST_USER)


class TestGetAuthUrl(IntegrationTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.account = _ensure_account()
        make_test_user(TEST_USER_2)

    def test_throws_does_not_exist_when_gmail_account_missing(self):
        """get_auth_url raises DoesNotExistError when the supplied Gmail Account doesn't exist."""
        with as_user(TEST_USER):
            with self.assertRaises(frappe.DoesNotExistError):
                get_auth_url("no-such-account@example.com")

    def test_throws_permission_error_when_session_user_lacks_write_permission(self):
        """get_auth_url raises PermissionError when frappe.has_permission returns False for write on the account."""
        with as_user(TEST_USER):
            with patch(f"{OAUTH_MODULE}.frappe.has_permission", return_value=False):
                with self.assertRaises(frappe.PermissionError):
                    get_auth_url(self.account.name)

    def test_throws_permission_error_when_session_user_is_not_linked_user(self):
        """get_auth_url raises PermissionError when frappe.session.user is not the account's linked_user."""
        with as_user(TEST_USER_2):
            with patch(f"{OAUTH_MODULE}.frappe.has_permission", return_value=True):
                with self.assertRaises(frappe.PermissionError):
                    get_auth_url(self.account.name)

    def test_returns_consent_url_with_required_oauth_parameters(self):
        """A successful get_auth_url returns a Google consent URL with the required OAuth params and configured client_id."""
        with as_user(TEST_USER):
            with change_settings("Google Settings", client_id="test-client-id"):
                with patch(f"{OAUTH_MODULE}.frappe.has_permission", return_value=True):
                    result = get_auth_url(self.account.name)
        url = result["url"]
        self.assertIn("https://accounts.google.com/o/oauth2/v2/auth", url)
        self.assertIn("access_type=offline", url)
        self.assertIn("response_type=code", url)
        self.assertIn("prompt=consent", url)
        self.assertIn("client_id=test-client-id", url)
        self.assertIn("https://www.googleapis.com/auth/gmail.readonly", url)
        self.assertIn(
            "/api/method/frappe_gmail_thread.api.oauth.callback",
            url,
        )


class TestCallback(IntegrationTestCase):
    def test_throws_does_not_exist_when_session_user_has_no_gmail_account(self):
        """callback raises DoesNotExistError when frappe.session.user has no Gmail Account row."""
        no_acct_user = "test_fgt_no_acct@example.com"
        make_test_user(no_acct_user)
        with as_user(no_acct_user):
            with self.assertRaises(frappe.DoesNotExistError):
                callback("any-code")

    def test_throws_permission_error_when_no_write_permission(self):
        """callback raises PermissionError when has_permission returns False for write."""
        _ensure_account()
        with as_user(TEST_USER):
            with patch(f"{OAUTH_MODULE}.frappe.has_permission", return_value=False):
                with self.assertRaises(frappe.PermissionError):
                    callback("any-code")


class TestAuthorizeAccess(IntegrationTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.account = _ensure_account()

    def test_saves_refresh_token_and_authorization_code_on_successful_exchange(self):
        """authorize_access stores refresh_token + authorization_code on the Gmail Account after a successful Google OAuth exchange."""
        frappe.db.set_value(
            "Gmail Account",
            self.account.name,
            {"refresh_token": "", "authorization_code": ""},
        )
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "access_token": "at-1",
            "refresh_token": "rt-fresh",
        }
        with as_user("Administrator"):
            with change_settings(
                "Google Settings", client_id="cid", client_secret="csec"
            ):
                with (
                    patch(f"{OAUTH_MODULE}.requests.post", return_value=mock_response),
                    patch(f"{OAUTH_MODULE}.build"),
                    patch(f"{OAUTH_MODULE}.check_gmail_object"),
                ):
                    authorize_access(TEST_USER, code="auth-code-1")
        saved = frappe.get_doc("Gmail Account", self.account.name)
        self.assertEqual(
            saved.get_password("refresh_token", raise_exception=False), "rt-fresh"
        )
        self.assertEqual(
            saved.get_password("authorization_code", raise_exception=False),
            "auth-code-1",
        )


class TestCheckGmailObject(IntegrationTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.account = _ensure_account()

    def test_throws_when_authorized_email_does_not_match_linked_user(self):
        """check_gmail_object throws when the Gmail-reported email doesn't match the account's linked_user."""
        mismatch_user = "test_fgt_mismatch@example.com"
        make_test_user(mismatch_user)
        gmail = MagicMock()
        gmail.users().getProfile().execute.return_value = {
            "emailAddress": mismatch_user
        }
        with self.assertRaises(frappe.ValidationError):
            check_gmail_object(self.account, gmail)

    def test_throws_authorization_expired_on_invalid_grant(self):
        """check_gmail_object throws a clear 'authorization expired' error when Google returns invalid_grant."""
        gmail = MagicMock()
        gmail.users().getProfile().execute.side_effect = Exception("invalid_grant")
        with self.assertRaises(frappe.ValidationError) as ctx:
            check_gmail_object(self.account, gmail)
        self.assertIn("authorization has expired", str(ctx.exception))


class TestGetAccessToken(IntegrationTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.account = _ensure_account()

    def test_raises_validation_error_when_refresh_token_missing(self):
        """get_access_token raises ValidationError when the Gmail Account has no refresh_token."""
        frappe.db.set_value("Gmail Account", self.account.name, "refresh_token", "")
        account = frappe.get_doc("Gmail Account", self.account.name)
        with self.assertRaises(frappe.ValidationError):
            get_access_token(account)
