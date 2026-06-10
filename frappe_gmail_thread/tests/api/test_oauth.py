from unittest.mock import MagicMock, patch

import frappe
from frappe.tests import IntegrationTestCase, change_settings

from frappe_gmail_thread.api.oauth import (
    authorize_access,
    callback,
    check_gmail_object,
    disable_pubsub,
    enable_pubsub,
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


def _reset_account_with_labels(*, gmail_enabled=1, refresh_token="rt", labels=()):
    """Reset the cached Gmail Account into a known state for pubsub tests, replacing its labels child table."""
    account = _ensure_account()
    frappe.db.set_value(
        "Gmail Account",
        account.name,
        {"gmail_enabled": gmail_enabled, "refresh_token": refresh_token},
    )
    frappe.db.delete("Gmail Label", {"parent": account.name})
    for idx, (label_id, enabled) in enumerate(labels, start=1):
        row = frappe.get_doc(
            {
                "doctype": "Gmail Label",
                "parent": account.name,
                "parenttype": "Gmail Account",
                "parentfield": "labels",
                "idx": idx,
                "label_id": label_id,
                "label_name": label_id,
                "enabled": 1 if enabled else 0,
            }
        )
        row.flags.ignore_validate = True
        row.db_insert()
    return frappe.get_doc("Gmail Account", account.name)


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
                "Google Settings", enable=1, client_id="cid", client_secret="csec"
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


class TestEnablePubsub(IntegrationTestCase):
    def test_returns_false_when_gmail_not_enabled(self):
        """enable_pubsub returns False (no-op) when gmail_enabled=0."""
        account = _reset_account_with_labels(gmail_enabled=0)
        with change_settings(
            "Google Settings",
            custom_gmail_sync_in_realtime=1,
            custom_gmail_pubsub_topic="topic",
        ):
            with patch(f"{OAUTH_MODULE}.get_gmail_object") as mock_get:
                result = enable_pubsub(account)
        self.assertEqual(result, False)
        mock_get.assert_not_called()

    def test_returns_false_when_realtime_disabled(self):
        """enable_pubsub returns False (no-op) when custom_gmail_sync_in_realtime=0."""
        account = _reset_account_with_labels(gmail_enabled=1)
        with change_settings(
            "Google Settings",
            custom_gmail_sync_in_realtime=0,
            custom_gmail_pubsub_topic="topic",
        ):
            with patch(f"{OAUTH_MODULE}.get_gmail_object") as mock_get:
                result = enable_pubsub(account)
        self.assertEqual(result, False)
        mock_get.assert_not_called()

    def test_throws_when_no_refresh_token(self):
        """enable_pubsub throws when account has no refresh_token."""
        account = _reset_account_with_labels(gmail_enabled=1, refresh_token="")
        with change_settings(
            "Google Settings",
            custom_gmail_sync_in_realtime=1,
            custom_gmail_pubsub_topic="topic",
        ):
            with self.assertRaises(frappe.ValidationError):
                enable_pubsub(account)

    def test_throws_when_no_pubsub_topic_configured(self):
        """enable_pubsub throws when Google Settings has no custom_gmail_pubsub_topic."""
        account = _reset_account_with_labels(gmail_enabled=1)
        with change_settings(
            "Google Settings",
            custom_gmail_sync_in_realtime=1,
            custom_gmail_pubsub_topic="",
        ):
            with self.assertRaises(frappe.ValidationError):
                enable_pubsub(account)

    def test_calls_watch_with_enabled_labels_and_appends_sent(self):
        """enable_pubsub calls users().watch() with the enabled label_ids plus SENT (appended if absent) and the configured topic."""
        account = _reset_account_with_labels(
            gmail_enabled=1, labels=(("INBOX", True), ("STARRED", False))
        )
        gmail = MagicMock()
        with change_settings(
            "Google Settings",
            custom_gmail_sync_in_realtime=1,
            custom_gmail_pubsub_topic="projects/x/topics/y",
        ):
            with patch(f"{OAUTH_MODULE}.get_gmail_object", return_value=gmail):
                enable_pubsub(account)
        body = gmail.users().watch.call_args.kwargs["body"]
        self.assertEqual(set(body["labelIds"]), {"INBOX", "SENT"})
        self.assertEqual(body["topicName"], "projects/x/topics/y")
        self.assertEqual(body["labelFilterBehavior"], "include")

    def test_does_not_duplicate_sent_when_already_in_enabled_labels(self):
        """enable_pubsub does not duplicate SENT when it is already in the enabled label_ids."""
        account = _reset_account_with_labels(
            gmail_enabled=1, labels=(("INBOX", True), ("SENT", True))
        )
        gmail = MagicMock()
        with change_settings(
            "Google Settings",
            custom_gmail_sync_in_realtime=1,
            custom_gmail_pubsub_topic="topic",
        ):
            with patch(f"{OAUTH_MODULE}.get_gmail_object", return_value=gmail):
                enable_pubsub(account)
        body = gmail.users().watch.call_args.kwargs["body"]
        self.assertEqual(body["labelIds"].count("SENT"), 1)


class TestDisablePubsub(IntegrationTestCase):
    def test_returns_false_when_gmail_not_enabled(self):
        """disable_pubsub returns False (no-op) when gmail_enabled=0."""
        account = _reset_account_with_labels(gmail_enabled=0)
        with change_settings(
            "Google Settings",
            custom_gmail_sync_in_realtime=0,
            custom_gmail_pubsub_topic="topic",
        ):
            with patch(f"{OAUTH_MODULE}.get_gmail_object") as mock_get:
                result = disable_pubsub(account)
        self.assertEqual(result, False)
        mock_get.assert_not_called()

    def test_returns_false_when_realtime_still_enabled(self):
        """disable_pubsub returns False (no-op) when custom_gmail_sync_in_realtime is still 1."""
        account = _reset_account_with_labels(gmail_enabled=1)
        with change_settings(
            "Google Settings",
            custom_gmail_sync_in_realtime=1,
            custom_gmail_pubsub_topic="topic",
        ):
            with patch(f"{OAUTH_MODULE}.get_gmail_object") as mock_get:
                result = disable_pubsub(account)
        self.assertEqual(result, False)
        mock_get.assert_not_called()

    def test_calls_stop_when_account_enabled_and_realtime_disabled(self):
        """disable_pubsub calls users().stop() when gmail_enabled=1 and realtime=0."""
        account = _reset_account_with_labels(gmail_enabled=1)
        gmail = MagicMock()
        with change_settings(
            "Google Settings",
            custom_gmail_sync_in_realtime=0,
            custom_gmail_pubsub_topic="topic",
        ):
            with patch(f"{OAUTH_MODULE}.get_gmail_object", return_value=gmail):
                disable_pubsub(account)
        gmail.users().stop.assert_called_once_with(userId="me")
