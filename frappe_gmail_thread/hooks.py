app_name = "frappe_gmail_thread"
app_title = "Frappe Gmail Thread"
app_publisher = "rtCamp"
app_description = "This app is used to display emails as threads, just like Gmail does."
app_email = "frappe@rtcamp.com"
app_license = "agpl-3.0"

# Apps
# ------------------

# required_apps = []

# Each item in the list will be shown as an app in the apps page
# add_to_apps_screen = [
# 	{
# 		"name": "frappe_gmail_thread",
# 		"logo": "/assets/frappe_gmail_thread/logo.png",
# 		"title": "Frappe Gmail Thread",
# 		"route": "/frappe_gmail_thread",
# 		"has_permission": "frappe_gmail_thread.api.permission.has_app_permission"
# 	}
# ]

# Includes in <head>
# ------------------

# include js, css files in header of desk.html
# app_include_css = "/assets/frappe_gmail_thread/css/frappe_gmail_thread.css"
app_include_js = ["/assets/frappe_gmail_thread/js/activity.js"]

additional_timeline_content = {
    "*": ["frappe_gmail_thread.api.activity.get_linked_gmail_threads"],
}

# include js, css files in header of web template
# web_include_css = "/assets/frappe_gmail_thread/css/frappe_gmail_thread.css"
# web_include_js = "/assets/frappe_gmail_thread/js/frappe_gmail_thread.js"

# include custom scss in every website theme (without file extension ".scss")
# website_theme_scss = "frappe_gmail_thread/public/scss/website"

# include js, css files in header of web form
# webform_include_js = {"doctype": "public/js/doctype.js"}
# webform_include_css = {"doctype": "public/css/doctype.css"}

# include js in page
# page_js = {"page" : "public/js/file.js"}

# include js in doctype views
# doctype_js = {
#     "Email Account" : "override/js/email_account.js"
# }
# doctype_list_js = {"doctype" : "public/js/doctype_list.js"}
# doctype_tree_js = {"doctype" : "public/js/doctype_tree.js"}
# doctype_calendar_js = {"doctype" : "public/js/doctype_calendar.js"}

# Svg Icons
# ------------------
# include app icons in desk
# app_include_icons = "frappe_gmail_thread/public/icons.svg"

# Home Pages
# ----------

# application home page (will override Website Settings)
# home_page = "login"

# website user home page (by Role)
# role_home_page = {
# 	"Role": "home_page"
# }

# Generators
# ----------

# automatically create page for each record of this doctype
# website_generators = ["Web Page"]

# Jinja
# ----------

# add methods and filters to jinja environment
# jinja = {
# 	"methods": "frappe_gmail_thread.utils.jinja_methods",
# 	"filters": "frappe_gmail_thread.utils.jinja_filters"
# }

# Installation
# ------------

# before_install = "frappe_gmail_thread.install.before_install"
# after_install = "frappe_gmail_thread.install.after_install"

# Uninstallation
# ------------

# before_uninstall = "frappe_gmail_thread.uninstall.before_uninstall"
# after_uninstall = "frappe_gmail_thread.uninstall.after_uninstall"

# Integration Setup
# ------------------
# To set up dependencies/integrations with other apps
# Name of the app being installed is passed as an argument

# before_app_install = "frappe_gmail_thread.utils.before_app_install"
# after_app_install = "frappe_gmail_thread.utils.after_app_install"

# Integration Cleanup
# -------------------
# To clean up dependencies/integrations with other apps
# Name of the app being uninstalled is passed as an argument

# before_app_uninstall = "frappe_gmail_thread.utils.before_app_uninstall"
# after_app_uninstall = "frappe_gmail_thread.utils.after_app_uninstall"

# Desk Notifications
# ------------------
# See frappe.core.notifications.get_notification_config

# notification_config = "frappe_gmail_thread.notifications.get_notification_config"

# Permissions
# -----------
# Permissions evaluated in scripted ways

permission_query_conditions = {
    "Gmail Thread": "frappe_gmail_thread.frappe_gmail_thread.doctype.gmail_thread.gmail_thread.get_permission_query_conditions",
}

has_permission = {
    "Gmail Thread": "frappe_gmail_thread.frappe_gmail_thread.doctype.gmail_thread.gmail_thread.has_permission",
}

# DocType Class
# ---------------
# Override standard doctype classes

# override_doctype_class = {
# 	"ToDo": "custom_app.overrides.CustomToDo"
# }
extend_doctype_class = {
    "File": ["frappe_gmail_thread.overrides.file.GmailThreadAttachmentTolerance"]
}

# Document Events
# ---------------
# Hook on document methods and events

# doc_events = {
# 	"Communication": {
# 		"after_insert": "frappe_gmail_thread.doc_events.communication.after_insert",
# 	},
# }

# Fixtures
# ----------

fixtures = [
    {
        "dt": "Custom Field",
        "filters": [
            [
                "module",
                "in",
                ["Frappe Gmail Thread"],
            ]
        ],
    },
    # TODO: Q: Should we export permissions?
    # {
    #     "dt": "Custom DocPerm",
    #     "filters": [
    #         [
    #             "parent",
    #             "in",
    #             ["Gmail Account", "Gmail Thread"],
    #         ]
    #     ],
    # }
]

# Scheduled Tasks
# ---------------

scheduler_events = {
    # 	"all": [
    # 		"frappe_gmail_thread.tasks.all"
    # 	],
    "daily": ["frappe_gmail_thread.tasks.daily.enable_pubsub_everyday"],
    # 	"hourly": [
    # 		"frappe_gmail_thread.tasks.hourly"
    # 	],
    # 	"weekly": [
    # 		"frappe_gmail_thread.tasks.weekly"
    # 	],
    # 	"monthly": [
    # 		"frappe_gmail_thread.tasks.monthly"
    # 	],
    "cron": {
        "*/30 * * * *": ["frappe_gmail_thread.tasks.sync.sync_emails"],
    },
}

# Testing
# -------

# before_tests = "frappe_gmail_thread.install.before_tests"

# Overriding Methods
# ------------------------------
#
# override_whitelisted_methods = {
# 	"frappe.desk.doctype.event.event.get_events": "frappe_gmail_thread.event.get_events"
# }
#
# each overriding function accepts a `data` argument;
# generated from the base implementation of the doctype dashboard,
# along with any modifications made in other Frappe apps
# override_doctype_dashboards = {
# 	"Task": "frappe_gmail_thread.task.get_dashboard_data"
# }

# exempt linked doctypes from being automatically cancelled
#
# auto_cancel_exempted_doctypes = ["Auto Repeat"]

# Ignore links to specified DocTypes when deleting documents
# -----------------------------------------------------------

# ignore_links_on_delete = ["Communication", "ToDo"]

# Request Events
# ----------------
# before_request = ["frappe_gmail_thread.utils.before_request"]
# after_request = ["frappe_gmail_thread.utils.after_request"]

# Job Events
# ----------
# before_job = ["frappe_gmail_thread.utils.before_job"]
# after_job = ["frappe_gmail_thread.utils.after_job"]

# User Data Protection
# --------------------

# user_data_fields = [
# 	{
# 		"doctype": "{doctype_1}",
# 		"filter_by": "{filter_by}",
# 		"redact_fields": ["{field_1}", "{field_2}"],
# 		"partial": 1,
# 	},
# 	{
# 		"doctype": "{doctype_2}",
# 		"filter_by": "{filter_by}",
# 		"partial": 1,
# 	},
# 	{
# 		"doctype": "{doctype_3}",
# 		"strict": False,
# 	},
# 	{
# 		"doctype": "{doctype_4}"
# 	}
# ]

# Authentication and authorization
# --------------------------------

# auth_hooks = [
# 	"frappe_gmail_thread.auth.validate"
# ]

# Automatically update python controller files with type annotations for this app.
# export_python_type_annotations = True

# default_log_clearing_doctypes = {
# 	"Logging DocType Name": 30  # days to retain logs
# }
