frappe.ui.form.on("Google Settings", {
  custom_gmail_pubsub_generate_token(frm) {
    // A random shared secret, generated in the browser so it is never a round-trip.
    const bytes = crypto.getRandomValues(new Uint8Array(24));
    const token = Array.from(bytes, (b) =>
      b.toString(16).padStart(2, "0"),
    ).join("");
    frm.set_value("custom_gmail_pubsub_token", token);

    const url = `${window.location.origin}/api/method/frappe_gmail_thread.api.pubsub.callback?token=${token}`;
    frappe.msgprint({
      title: __("Pub/Sub Push Endpoint"),
      indicator: "green",
      message:
        __(
          "Save this form, then set your Pub/Sub subscription's push endpoint to:",
        ) +
        `<br><br><code style="user-select:all;word-break:break-all">${frappe.utils.escape_html(
          url,
        )}</code>`,
    });
  },
});
