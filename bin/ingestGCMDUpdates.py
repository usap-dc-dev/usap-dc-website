import ingestGCMDDataFormat as df
import ingestGCMDInstruments as inst
import ingestGCMDPlatforms as plat
import ingestGCMDTime as time
import gmail_functions as gmail
import socket

msg_subject = "%s: GCMD Keyword Update" % socket.gethostname().upper()
#print(msg_subject)

df_msg = df.update_db()
#print(df_msg)
inst_msg = inst.update_db()
#print(inst_msg)
plat_msg = plat.update_db()
#print(plat_msg)
time_msg = time.update_db()
#print(time_msg)

msg_text = "<br><br>".join([df_msg, inst_msg, plat_msg, time_msg])

msg_sent = gmail.send_gmail_message("info@usap-dc.org", ["info@usap-dc.org"], msg_subject, msg_text, None)