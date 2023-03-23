alter table dataset_fairness
	alter column file_name_check TYPE text,
	alter column file_format_check TYPE text,
	alter column file_organization_check TYPE text,
	alter column table_header_check TYPE text,
	alter column data_content_check TYPE text,
	alter column data_process_check TYPE text,
	alter column data_acquisition_check TYPE text,
	alter column data_spatial_check TYPE text,
	alter column data_variable_check TYPE text,
	alter column data_issues_check TYPE text,
	alter column data_ref_check TYPE text,
	alter column abstract_check TYPE text,
	alter column data_temporal_check TYPE text;

alter table dataset_fairness
	alter column file_name_check set default '0',
	alter column file_format_check set default '0',
	alter column file_organization_check set default '0',
	alter column table_header_check set default '0',
	alter column data_content_check set default '0',
	alter column data_process_check set default '0',
	alter column data_acquisition_check set default '0',
	alter column data_spatial_check set default '0',
	alter column data_variable_check set default '0',
	alter column data_issues_check set default '0',
	alter column data_ref_check set default '0',
	alter column abstract_check set default '0',
	alter column data_temporal_check set default '0';

update dataset_fairness set file_name_check = '1' where file_name_check = 'true';
update dataset_fairness set file_name_check = '0' where file_name_check = 'false';
update dataset_fairness set file_format_check = '1' where file_format_check = 'true';
update dataset_fairness set file_format_check = '0' where file_format_check = 'false';
update dataset_fairness set file_organization_check = '1' where file_organization_check = 'true';
update dataset_fairness set file_organization_check = '0' where file_organization_check = 'false';
update dataset_fairness set table_header_check = '1' where table_header_check = 'true';
update dataset_fairness set table_header_check = '0' where table_header_check = 'false';
update dataset_fairness set data_content_check = '1' where data_content_check = 'true';
update dataset_fairness set data_content_check = '0' where data_content_check = 'false';
update dataset_fairness set data_process_check = '1' where data_process_check = 'true';
update dataset_fairness set data_process_check = '0' where data_process_check = 'false';
update dataset_fairness set data_acquisition_check = '1' where data_acquisition_check = 'true';
update dataset_fairness set data_acquisition_check = '0' where data_acquisition_check = 'false';
update dataset_fairness set data_spatial_check = '1' where data_spatial_check = 'true';
update dataset_fairness set data_spatial_check = '0' where data_spatial_check = 'false';
update dataset_fairness set data_variable_check = '1' where data_variable_check = 'true';
update dataset_fairness set data_variable_check = '0' where data_variable_check = 'false';
update dataset_fairness set data_issues_check = '1' where data_issues_check = 'true';
update dataset_fairness set data_issues_check = '0' where data_issues_check = 'false';
update dataset_fairness set data_ref_check = '1' where data_ref_check = 'true';
update dataset_fairness set data_ref_check = '0' where data_ref_check = 'false';
update dataset_fairness set abstract_check = '1' where abstract_check = 'true';
update dataset_fairness set abstract_check = '0' where abstract_check = 'false';
update dataset_fairness set data_temporal_check = '1' where data_temporal_check = 'true';
update dataset_fairness set data_temporal_check = '0' where data_temporal_check = 'false';

alter table dataset_fairness
	alter column file_name_check TYPE smallint USING file_name_check::smallint,
	alter column file_format_check TYPE smallint USING file_format_check::smallint,
	alter column file_organization_check TYPE smallint USING file_organization_check::smallint,
	alter column table_header_check TYPE smallint USING table_header_check::smallint,
	alter column data_content_check TYPE smallint USING data_content_check::smallint,
	alter column data_process_check TYPE smallint USING data_process_check::smallint,
	alter column data_acquisition_check TYPE smallint USING data_acquisition_check::smallint,
	alter column data_spatial_check TYPE smallint USING data_spatial_check::smallint,
	alter column data_variable_check TYPE smallint USING data_variable_check::smallint,
	alter column data_issues_check TYPE smallint USING data_issues_check::smallint,
	alter column data_ref_check TYPE smallint USING data_ref_check::smallint,
	alter column abstract_check TYPE smallint USING abstract_check::smallint,
	alter column data_temporal_check TYPE smallint USING data_temporal_check::smallint;
