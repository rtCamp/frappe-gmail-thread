from unittest.mock import MagicMock, patch

from frappe.tests import IntegrationTestCase

from frappe_gmail_thread.doc_events.google_settings import on_update
from frappe_gmail_thread.tests import (
    TEST_USER,
    as_user,
    make_test_gmail_account,
    make_test_user,
)

GOOGLE_SETTINGS_DOC_EVENT_MODULE = "frappe_gmail_thread.doc_events.google_settings"


class TestOnUpdate(IntegrationTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        make_test_user(TEST_USER)
        with as_user(TEST_USER):
            make_test_gmail_account(linked_user=TEST_USER)

    def test_calls_enable_pubsub_when_google_settings_realtime_truthy(self):
        """When Google Settings.custom_gmail_sync_in_realtime is truthy and the field changed, on_update calls enable_pubsub for every Gmail Account. The dispatch decision must read off the Google Settings doc (the on_update param), not the iterated Gmail Account."""
        gs_doc = MagicMock()
        gs_doc.has_value_changed.return_value = True
        gs_doc.custom_gmail_sync_in_realtime = 1
        with (
            patch(
                f"{GOOGLE_SETTINGS_DOC_EVENT_MODULE}.enable_pubsub"
            ) as mock_enable,
            patch(
                f"{GOOGLE_SETTINGS_DOC_EVENT_MODULE}.disable_pubsub"
            ) as mock_disable,
        ):
            on_update(gs_doc)
        mock_enable.assert_called_once()
        mock_disable.assert_not_called()

    def test_calls_disable_pubsub_when_google_settings_realtime_falsy(self):
        """When Google Settings.custom_gmail_sync_in_realtime is falsy and the field changed, on_update calls disable_pubsub for every Gmail Account."""
        gs_doc = MagicMock()
        gs_doc.has_value_changed.return_value = True
        gs_doc.custom_gmail_sync_in_realtime = 0
        with (
            patch(
                f"{GOOGLE_SETTINGS_DOC_EVENT_MODULE}.enable_pubsub"
            ) as mock_enable,
            patch(
                f"{GOOGLE_SETTINGS_DOC_EVENT_MODULE}.disable_pubsub"
            ) as mock_disable,
        ):
            on_update(gs_doc)
        mock_enable.assert_not_called()
        mock_disable.assert_called_once()

    def test_does_nothing_when_realtime_setting_unchanged(self):
        """on_update is a no-op when has_value_changed('custom_gmail_sync_in_realtime') returns False."""
        gs_doc = MagicMock()
        gs_doc.has_value_changed.return_value = False
        with (
            patch(
                f"{GOOGLE_SETTINGS_DOC_EVENT_MODULE}.enable_pubsub"
            ) as mock_enable,
            patch(
                f"{GOOGLE_SETTINGS_DOC_EVENT_MODULE}.disable_pubsub"
            ) as mock_disable,
        ):
            on_update(gs_doc)
        mock_enable.assert_not_called()
        mock_disable.assert_not_called()
