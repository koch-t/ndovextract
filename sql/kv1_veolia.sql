alter table pool drop constraint pool_version_fkey;
alter table link drop constraint link_pkey;
alter table link add constraint link_pkey PRIMARY KEY ("version", "dataownercode", "userstopcodebegin", "userstopcodeend", "validfrom");
alter table pool add constraint pool_dataownercode_fkey FOREIGN KEY (Version, DataOwnerCode, UserStopCodeBegin, UserStopCodeEnd, LinkValidFrom) 
REFERENCES 
link (Version, DataOwnerCode, UserStopCodeBegin, UserStopCodeEnd, ValidFrom);

alter table pool drop constraint pool_pkey;
alter table pool add constraint pool_pkey PRIMARY KEY (Version, DataOwnerCode, UserStopCodeBegin, UserStopCodeEnd, LinkValidFrom, PointDataOwnerCode, 
PointCode);

alter table link drop column transporttype;
alter table pool drop column transporttype;

alter table pool_delta drop constraint pool_delta_dataownercode_fkey;
alter table link_delta drop constraint link_delta_pkey;
alter table link_delta add constraint link_delta_pkey PRIMARY KEY ("dataownercode", "userstopcodebegin", "userstopcodeend", "validfrom");
alter table pool_delta add constraint pool_dataownercode_fkey FOREIGN KEY (DataOwnerCode, UserStopCodeBegin, UserStopCodeEnd, LinkValidFrom) 
REFERENCES 
link_delta ( DataOwnerCode, UserStopCodeBegin, UserStopCodeEnd, ValidFrom);

alter table pool_delta drop constraint pool_delta_pkey;
alter table pool_delta add constraint pool_delta_pkey PRIMARY KEY (DataOwnerCode, UserStopCodeBegin, UserStopCodeEnd, LinkValidFrom, 
PointDataOwnerCode, 
PointCode);

alter table link_delta drop column transporttype;
alter table pool_delta drop column transporttype;

alter table line drop column transporttype;
alter table line_delta drop column transporttype;
alter table dest drop column relevantdestnamedetail;
alter table dest_delta drop column relevantdestnamedetail;
alter table usrstop drop column userstoptype;
alter table usrstop_delta drop column userstoptype;
alter table jopatili drop column productformulatype;
alter table jopatili_delta drop column productformulatype;
alter table pujopass drop column dataownerisoperator;
alter table pujopass_delta drop column dataownerisoperator;
alter table pujopass_delta drop column wheelchairaccessible;
alter table pujopass drop column wheelchairaccessible;
alter table line alter linevetagnumber type VARCHAR(3);
alter table line_delta alter linevetagnumber type VARCHAR(3);
alter table jopatili drop constraint jopatili_version_fkey;
alter table jopatili_delta drop constraint jopatili_delta_dataownercode_fkey;
