import ingestGCMDDataFormat as df
import ingestGCMDInstruments as inst
import ingestGCMDPlatforms as plat
import ingestGCMDTime as time
import gmail_functions as gmail

df_msg = df.update_db()
inst_msg = inst.update_db()
plat_msg = plat.update_db()
time_msg = time.update_db()

msg_text = "\n".join([df_msg, inst_msg, plat_msg, time_msg])
msg_subject = "GCMD Keyword Update"

print(msg_subject)
print(msg_text)

msg_sent = gmail.send_gmail_message("info@usap-dc.org", ["info@usap-dc.org"], msg_subject, msg_text, None)
