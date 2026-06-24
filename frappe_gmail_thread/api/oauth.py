from urllib.parse import quote

import frappe
import frappe.utils
import google.oauth2.credentials
import requests
from frappe import _
from frappe.integrations.google_oauth import GoogleOAuth
from googleapiclient.discovery import build

SCOPES = "https://www.googleapis.com/auth/gmail.readonly"


def get_authentication_url(client_id=None, redirect_uri=None):
    return {
        "url": "https://accounts.google.com/o/oauth2/v2/auth?access_type=offline&response_type=code&prompt=consent&client_id={}&include_granted_scopes=true&scope={}&redirect_uri={}".format(
            client_id, SCOPES, redirect_uri
        )
    }


@frappe.whitelist()
def get_auth_url(doc: str | int):
    current_user = frappe.session.user
    current_user = frappe.get_doc("User", current_user)
    try:
        doc = frappe.get_doc("Gmail Account", doc)
    except frappe.DoesNotExistError:
        frappe.throw(
            _("Gmail Account not found. If not saved, please save the document first."),
            frappe.DoesNotExistError,
        )
    # check permission
    if not frappe.has_permission(doctype="Gmail Account", doc=doc.name, ptype="write"):
        frappe.throw(
            _("You don't have permission to access this document"),
            frappe.PermissionError,
        )
    # check if user email is same as email account email
    if current_user.name != doc.linked_user:
        frappe.throw(
            _("You can only authorize access for your own email account"),
            frappe.PermissionError,
        )
    google_settings = frappe.get_single("Google Settings")
    client_id = google_settings.client_id
    redirect_uri = frappe.utils.get_url(
        "/api/method/frappe_gmail_thread.api.oauth.callback"
    )
    return get_authentication_url(client_id, redirect_uri)


def authorize_access(user, code=None, reauthorize=None):
    """
    If no Authorization code get it from Google and then request for Refresh Token.
    """
    google_settings = frappe.get_doc("Google Settings")
    gmail_account = frappe.get_doc("Gmail Account", {"linked_user": user})
    gmail_account.check_permission("write")

    redirect_uri = frappe.utils.get_url(
        "/api/method/frappe_gmail_thread.api.oauth.callback"
    )

    if not code or reauthorize:
        return get_authentication_url(
            client_id=google_settings.client_id, redirect_uri=redirect_uri
        )
    else:
        try:
            data = {
                "code": code,
                "client_id": google_settings.client_id,
                "client_secret": google_settings.get_password(
                    fieldname="client_secret", raise_exception=False
                ),
                "redirect_uri": redirect_uri,
                "grant_type": "authorization_code",
            }
            r = requests.post(GoogleOAuth.OAUTH_URL, data=data).json()

            if "refresh_token" in r:
                credentials_dict = {
                    "token": r["access_token"],
                    "refresh_token": r["refresh_token"],
                    "token_uri": GoogleOAuth.OAUTH_URL,
                    "client_id": google_settings.client_id,
                    "client_secret": google_settings.get_password(
                        fieldname="client_secret", raise_exception=False
                    ),
                    "scopes": [SCOPES],
                }

                credentials = google.oauth2.credentials.Credentials(**credentials_dict)
                gmail = build(
                    serviceName="gmail", version="v1", credentials=credentials
                )

                check_gmail_object(gmail_account, gmail)

                gmail_account.reload()
                gmail_account.refresh_token = r["refresh_token"]
                gmail_account.authorization_code = code
                gmail_account.save(ignore_permissions=True)
                frappe.db.commit()  # nosemgrep: Committing manually because it's a part of a GET request

            frappe.local.response["type"] = "redirect"
            frappe.local.response[
                "location"
            ] = f"/app/gmail-account/{quote(gmail_account.name)}"

            frappe.msgprint(_("Gmail has been configured."))
        except Exception as e:
            frappe.throw(e)


@frappe.whitelist()
def callback(code: str | int):
    """
    Authorization code is sent to callback as per the API configuration
    """
    user = frappe.session.user
    # get gmail account using filter
    gmail_account = frappe.get_doc("Gmail Account", {"linked_user": user})

    # check user email same as callback email
    if not gmail_account:
        frappe.throw(
            _("Gmail Account not found. If not saved, please save the document first."),
            frappe.DoesNotExistError,
        )
    if not frappe.has_permission(
        doctype="Gmail Account", doc=gmail_account.name, ptype="write"
    ):
        frappe.throw(
            _("You don't have permission to access this document"),
            frappe.PermissionError,
        )

    authorize_access(user, code)


def enable_pubsub(gmail_account):
    google_settings = frappe.get_single("Google Settings")
    if (
        not gmail_account.gmail_enabled
        or not google_settings.custom_gmail_sync_in_realtime
    ):
        return False
    if not gmail_account.refresh_token:
        frappe.throw(
            _("Please authorize Gmail by clicking on 'Authorize Gmail' button.")
        )
    if not google_settings.custom_gmail_pubsub_topic:
        frappe.throw(_("Please configure PubSub in Google Settings."))
    gmail = get_gmail_object(gmail_account)
    topic = google_settings.custom_gmail_pubsub_topic
    label_ids = [x.label_id for x in gmail_account.labels if x.enabled]
    if not label_ids:
        label_ids = [x.label_id for x in gmail_account.labels if x.enabled]
    if "SENT" not in label_ids:
        label_ids.append("SENT")
    body = {
        "labelIds": label_ids,
        "topicName": topic,
        "labelFilterBehavior": "include",
    }
    gmail.users().watch(userId="me", body=body).execute()


def disable_pubsub(gmail_account):
    google_settings = frappe.get_single("Google Settings")
    if not gmail_account.gmail_enabled or google_settings.custom_gmail_sync_in_realtime:
        return False
    if not gmail_account.refresh_token:
        frappe.throw(
            _("Please authorize Gmail by clicking on 'Authorize Gmail' button.")
        )
    if not google_settings.custom_gmail_pubsub_topic:
        frappe.throw(_("Please configure PubSub in Email Account."))
    gmail = get_gmail_object(gmail_account)
    gmail.users().stop(userId="me").execute()


def get_access_token(gmail_account):
    google_settings = frappe.get_single("Google Settings")
    if isinstance(gmail_account, str):
        gmail_account = frappe.get_doc("Gmail Account", gmail_account)

    if not gmail_account.refresh_token:
        button_label = frappe.bold(_("Authorize Gmail"))
        raise frappe.ValidationError(
            _("Click on {0} in Gmail Account to generate Refresh Token.").format(
                button_label
            )
        )

    data = {
        "client_id": google_settings.client_id,
        "client_secret": google_settings.get_password(
            fieldname="client_secret", raise_exception=False
        ),
        "refresh_token": gmail_account.get_password(
            fieldname="refresh_token", raise_exception=False
        ),
        "grant_type": "refresh_token",
        "scope": SCOPES,
    }

    try:
        r = requests.post(GoogleOAuth.OAUTH_URL, data=data).json()
    except requests.exceptions.HTTPError:
        button_label = frappe.bold(_("Authorize Gmail"))
        frappe.throw(
            _(
                "Something went wrong during the token generation. Click on {0} in Gmail Account to generate Refresh Token."
            ).format(button_label)
        )

    return r.get("access_token")


def get_gmail_object(gmail_account):
    """
    Returns an object of Google Mail along with Google Mail doc.
    """
    google_settings = frappe.get_doc("Google Settings")
    if isinstance(gmail_account, str):
        account = frappe.get_doc("Gmail Account", gmail_account)
    else:
        account = gmail_account

    credentials_dict = {
        "token": get_access_token(account),
        "refresh_token": account.get_password(
            fieldname="refresh_token", raise_exception=False
        ),
        "token_uri": GoogleOAuth.OAUTH_URL,
        "client_id": google_settings.client_id,
        "client_secret": google_settings.get_password(
            fieldname="client_secret", raise_exception=False
        ),
        "scopes": [SCOPES],
    }

    credentials = google.oauth2.credentials.Credentials(**credentials_dict)
    gmail = build(serviceName="gmail", version="v1", credentials=credentials)

    return gmail


def check_gmail_object(account, gmail):
    try:
        gmail = gmail.users().getProfile(userId="me").execute()
        # get email address from the response
        email = gmail["emailAddress"]
        # check if email address is same as the email account email
    except Exception as e:
        if "invalid_grant" in str(e):
            button_label = frappe.bold(_("Authorize Gmail"))
            frappe.throw(
                _(
                    "Your Gmail authorization has expired. Click on {0} in Email Account to re-authorize."
                ).format(button_label)
            )
        raise
    user = frappe.get_doc("User", {"email": email})
    if not user:
        frappe.throw(_("Email address in Gmail Account does not match with any User."))
    if user.name != account.linked_user:
        frappe.throw(_("You can only authorize access for your own email account"))
    return gmail
