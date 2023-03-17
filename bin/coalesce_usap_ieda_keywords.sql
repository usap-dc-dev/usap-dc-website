INSERT INTO keyword_usap SELECT keyword_id, keyword_label, keyword_type_id, keyword_description FROM keyword_ieda WHERE keyword_label NOT IN (select keyword_label from keyword_usap);
update keyword_usap set keyword_type_id = keyword_description where keyword_id like 'ik%';
update keyword_usap set keyword_description = keyword_altlabel where keyword_id like 'ik%';
update keyword_usap set keyword_altlabel = null where keyword_id like 'ik%';