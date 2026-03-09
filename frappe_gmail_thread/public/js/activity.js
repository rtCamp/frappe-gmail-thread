frappe.router.on("change", page_changed);

// This will load the user info for the Gmail Thread activity, so that the user's profile picture can be displayed
function page_changed(event) {
  frappe.after_ajax(function () {
    var route = frappe.get_route();

    if (route[0] == "Form") {
      frappe.ui.form.on(route[1], {
        refresh: function (frm) {
          if (
            frm.timeline &&
            frm.timeline.doc_info &&
            frm.timeline.doc_info.additional_timeline_content
          ) {
            let gthread_users = new Set();
            for (let activity of frm.timeline.doc_info.additional_timeline_content) {
              if (activity.doctype === "Gmail Thread") {
                gthread_users.add(activity.owner);
              }
            }
            gthread_users = Array.from(gthread_users);
            frappe
              .call({
                method: "frappe.desk.form.load.get_user_info_for_viewers",
                args: {
                  users: JSON.stringify(gthread_users),
                },
              })
              .then((r) => {
                const user_info = r.message;
                if ($.isEmptyObject(user_info)) return;
                frappe.update_user_info(user_info);
                if(frm.timeline){
                  frm.timeline.refresh();
                }
              });
          }
          frappe.realtime.on("gthread_new_email", function (data) {
            if(frm.timeline){
              frm.timeline.refresh();
            }
          });
        },
      });
    }
  });
}
