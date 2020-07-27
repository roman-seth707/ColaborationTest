from sqlalchemy import Column, String, Date, DateTime, Float, Integer, Unicode, TypeDecorator, Boolean, TIMESTAMP
from sqlalchemy.types import JSON
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy import ForeignKey
from sqlalchemy.orm import backref, relationship
from datamagic import aes_encrypt, aes_decrypt
from Leggero_DB import Base
# import json

# from Raptor.commons.config_reader import *
# LeggeroConfig = LgConfig().LeggeroConfig

skey = '\x10\x8fV\xea\xcd\xac\x92]\x03\xef\x04yu\xbb\xd7LQ\x88;\xbbG\x99\x9a\x0f'


class EncryptedValue(TypeDecorator):
    impl = Unicode

    def process_bind_param(self, value, dialect):
        return aes_encrypt(value)

    def process_result_value(self, value, dialect):
        return aes_decrypt(value)

# use this Decorator if working with ORACLE
# class JsonStringify(TypeDecorator):
#     dbtype = LeggeroConfig['LeggeroDB']['dbtype']
#     if dbtype == 'postgres':
#         impl = JSON
        
#         def process_bind_param(self, value, dialect):
#             return value

#         def process_result_value(self, value, dialect):
#             return value

#     elif dbtype == 'oracle':
#         impl = Unicode

#         def process_bind_param(self, value, dialect):
#             return json.dumps(value)

#         def process_result_value(self, value, dialect):
#             return json.loads(value)


class Connection(Base):
    __tablename__ = 'connections'
    con_id = Column(Integer, primary_key=True)
    name = Column(Unicode, unique=True)
    con_string = Column(Unicode)
    con_type = Column(Unicode)
    datasource = relationship('Datasource', backref=backref('connection'))


class Datasource(Base):
    __tablename__ = 'datasource'
    ds_id = Column(Integer, primary_key=True)
    name = Column(Unicode, unique=True)
    ds_table = Column(Unicode)
    ftype = Column(Unicode)
    connection_id = Column(Integer, ForeignKey('connections.con_id'))
    partition_col = Column(Integer)
    lowerbound = Column(Integer)
    upperbound = Column(Integer)
    numpartitions = Column(Integer)
    predicates = Column(Unicode)
    splitscheme = Column(Unicode)
    col_list = Column(Unicode)
    dep_stat = Column(Unicode)
    lgtables = relationship('Tables', backref=backref('tabdatasource'))


class Tables(Base):
    __tablename__ = 'lg_tables'
    id = Column(Integer, primary_key=True)
    name = Column(Unicode, unique=True)
    dep_stat = Column(Unicode)
    data_source_id = Column(Integer, ForeignKey('datasource.ds_id'))
    lgcolumns = relationship('TabCols', backref=backref('table'))


class AOFrmQry(Base):
    __tablename__ = 'lg_aofrmqry'
    id = Column(Integer, primary_key=True)
    name = Column(Unicode, unique=True)
    dep_stat = Column(Unicode)
    query_id = Column(Integer, ForeignKey('lg_query.id'))


class TabCols(Base):
    __tablename__ = 'lg_columns'
    id = Column(Integer, primary_key=True)
    name = Column(Unicode)
    name_in_ds = Column(Unicode)
    cast_type = Column(Unicode)
    decimals = Column(Integer)
    parent_id = Column(Integer, ForeignKey('lg_tables.id'))


class Views(Base):
    __tablename__ = 'lg_views'
    id = Column(Integer, primary_key=True)
    name = Column(Unicode)
    recfilter = Column(Unicode)
    dep_stat = Column(Unicode)
    viewjoins = relationship('ViewJoins', backref=backref('view'))
    viewcols = relationship('ViewCols', backref=backref('view'))


class ViewJoins(Base):
    __tablename__ = 'lg_view_tables'
    id = Column(Integer, primary_key=True)
    join_ds1 = Column(Unicode)
    join_column1 = Column(Unicode)
    join_ds2 = Column(Unicode)
    join_column2 = Column(Unicode)
    parent_id = Column(Integer, ForeignKey('lg_views.id'))


class ViewCols(Base):
    __tablename__ = 'lg_view_cols'
    id = Column(Integer, primary_key=True)
    name = Column(Unicode)
    ds_name = Column(Unicode)
    name_in_ds = Column(Unicode)
    cast_type = Column(Unicode)
    parent_id = Column(Integer, ForeignKey('lg_views.id'))


class LGQuery(Base):
    __tablename__ = 'lg_query'
    id = Column(Integer, primary_key=True)
    name = Column(Unicode)
    ao_name = Column(Unicode)
    tao_name = Column(Unicode)
    vao_name = Column(Unicode)
    group_cols = Column(Unicode)
    filter_cols = Column(Unicode)
    grp_filter = Column(Unicode)
    description = Column(Unicode)
    qry_string = Column(Unicode)
    param_val = Column(JSON)
    hidden_param_val = Column(JSON)
    dep_stat = Column(Unicode)
    selected_cols = Column(JSON)
    reports = relationship('LGReport', backref=backref('query'))
    reports = relationship('LGVInsights', backref=backref('query'))
    aofrmqry = relationship('AOFrmQry', backref=backref('aoquery'))
    is_filter_query = Column(Boolean, default=False)
    is_multilevel_query = Column(Boolean, default=False)


class LGReport(Base):
    __tablename__ = 'lg_reports'
    id = Column(Integer, primary_key=True)
    name = Column(Unicode)
    col_def = Column(JSON)
    param_def = Column(JSON)
    description = Column(Unicode)
    is_multi_level = Column(Boolean, default=False)
    query_id = Column(Integer, ForeignKey('lg_query.id'))


class LGReportDashboard(Base):
    __tablename__ = 'lg_report_dashboard'
    id = Column(Integer, primary_key=True)
    name = Column(Unicode)
    rep_name = Column(Unicode)
    rep_description = Column(Unicode)
    row_def = Column(JSON)
    dash_params = Column(JSON)


class LGReportDashboardGroup(Base):
    __tablename__ = 'lg_report_dashboard_group'
    id = Column(Integer, primary_key=True)
    name = Column(Unicode)
    rep_dashgroup_name = Column(Unicode)
    rep_dashgroup_desc = Column(Unicode)
    icon_class = Column(Unicode)


class LGVInsights(Base):
    __tablename__ = 'lg_vinsights'
    id = Column(Integer, primary_key=True)
    name = Column(Unicode)
    option_def = Column(JSON)
    description = Column(Unicode)
    vi_type = Column(Unicode)
    data_def = Column(JSON)
    query_id = Column(Integer, ForeignKey('lg_query.id'))
    child_id = Column(Integer)
    email_def = Column(JSONB)


class LGCompositeWidgets(Base):
    __tablename__ = 'lg_composite_widgets'
    id = Column(Integer, primary_key=True)
    name = Column(Unicode)
    description = Column(Unicode)
    data_def = Column(JSON)
    widget_def = Column(JSON)
    option_def = Column(JSON)
    type = Column(Unicode)
    query_id = Column(Integer, ForeignKey('lg_query.id'))


class LGDashboards(Base):
    __tablename__ = 'lg_dashboards'
    id = Column(Integer, primary_key=True)
    name = Column(Unicode)
    description = Column(Unicode)
    dtitle = Column(Unicode)
    row_def = Column(JSON)
    db_file = Column(Unicode)
    dash_params = Column(JSON)
    has_chart = Column(Boolean, default=False)
    has_report = Column(Boolean, default=False)
    has_widget = Column(Boolean, default=False)
    has_text = Column(Boolean, default=False)


class LGUserHomeDashboard(Base):
    __tablename__ = 'lg_user_home_dashboard'
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer)
    dashboard_id = Column(Integer)
    status = Column(Unicode)


class LGUser(Base):
    __tablename__ = 'lg_user'
    id = Column(Integer, primary_key=True)
    user_name = Column(Unicode)
    is_active = Column(Unicode)
    is_system = Column(Unicode)
    is_admin = Column(Unicode)
    pwd = Column(EncryptedValue)


class LGM2MRepgroupRep(Base):
    __tablename__ = 'lg_rgroup_report'
    id = Column(Integer, primary_key=True)
    report_id = Column(Integer)
    rgroup_id = Column(Integer)
    status = Column(Unicode)


class LGM2MRepgroupUser(Base):
    __tablename__ = 'lg_rgroup_user'
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer)
    rgroup_id = Column(Integer)
    status = Column(Unicode)


####    decleration for views
class LGUserReps(Base):
    __tablename__ = 'lg_user_reps'
    viewid = Column(Integer, primary_key=True)
    userid = Column(Integer)
    username = Column(Unicode)
    rgroupuserid = Column(Integer)
    rgroupuserstatus = Column(Unicode)
    rgroupid = Column(Integer)
    rgroupname = Column(Unicode)
    rgroupdesc = Column(Unicode)
    rgrouprepid = Column(Integer)
    rgrouprepstatus = Column(Unicode)
    repid = Column(Integer)
    repname = Column(Unicode)
    repdesc = Column(Unicode)
    query_id = Column(Integer)


class LGUserRepgroup(Base):
    __tablename__ = 'lg_user_repgroup'
    viewid = Column(Integer, primary_key=True)
    userid = Column(Integer)
    username = Column(Unicode)
    rgroupuserid = Column(Integer)
    rgroupuserstatus = Column(Unicode)
    rgroupid = Column(Integer)
    rgroupname = Column(Unicode)
    rgroupdesc = Column(Unicode)


class LGURepgroupReps(Base):
    __tablename__ = 'lg_repgroup_rep'
    viewid = Column(Integer, primary_key=True)
    rgroupid = Column(Integer)
    rgroupname = Column(Unicode)
    rgroupdesc = Column(Unicode)
    rgrouprepid = Column(Integer)
    rgrouprepstatus = Column(Unicode)
    repid = Column(Integer)
    repname = Column(Unicode)
    repdesc = Column(Unicode)
    query_id = Column(Integer)


class LGShowReport(Base):
    __tablename__ = 'lg_show_reps'
    viewid = Column(Integer, primary_key=True)
    id = Column(Integer)
    name = Column(Unicode)
    col_def = Column(Unicode)
    description = Column(Unicode)
    query_id = Column(Integer)
    username = Column(Unicode)


class LGShowDashboard(Base):
    __tablename__ = 'lg_show_dashboard'
    viewid = Column(Integer, primary_key=True)
    username = Column(Unicode)
    dashboard_group_id = Column(Integer)
    dashboard_id = Column(Integer)
    dashboard_name = Column(Unicode)
    order = Column(Integer)
    dashboard_title = Column(Unicode)
    dashboard_file_name = Column(Unicode)


class LGShowDashGroup(Base):
    __tablename__ = 'lg_show_dash_group'
    viewid = Column(Integer, primary_key=True)
    username = Column(Unicode)
    dashboard_group_id = Column(Integer)
    name = Column(Unicode)
    display_name = Column(Unicode)
    order = Column(Integer)



class LGShowReportDashboard(Base):
    __tablename__ = 'lg_show_report_dashboard'
    view_id = Column(Integer, primary_key=True)
    user_id = Column(Integer)
    user_name = Column(Unicode)
    rep_dashboard_group_id = Column(Integer)
    rep_dashboard_id = Column(Integer)
    dashboard_status = Column(Unicode)
    dashboard_order = Column(Integer)
    rep_name = Column(Unicode)
    rep_description = Column(Unicode)
    row_def = Column(JSON)
    dash_params = Column(JSON)


class LGShowReportDashGroup(Base):
    __tablename__ = 'lg_show_report_dashboard_group'
    view_id = Column(Integer, primary_key=True)
    user_id = Column(Integer)
    rep_dashboard_group_id = Column(Integer)
    user_name = Column(Unicode)
    rep_dashgroup_name = Column(Unicode)
    rep_dashgroup_desc = Column(Unicode)
    icon_class = Column(Unicode)
    status = Column(Unicode)
    order = Column(Integer)


class LGDashGroup(Base):
    __tablename__ = 'lg_dshb_group'
    id = Column(Integer, primary_key=True)
    name = Column(Unicode)
    description = Column(Unicode)
    display_name = Column(Unicode)
    icon_class = Column(Unicode)


class LGDashGroupMap(Base):
    __tablename__ = 'lg_dshbgroup_dashboard'
    id = Column(Integer, primary_key=True)
    dashboard_id = Column(Integer, ForeignKey('lg_dashboards.id'))
    dshbgroup_id = Column(Integer)
    status = Column(Unicode)
    order = Column(Integer)
    # dashboard = relationship('LGDashboards',backref=backref('dashboard_id'))

class LGReportDashboardToGroup(Base):
    __tablename__ = 'lg_rep_dashboard_to_dashgroup'
    id = Column(Integer, primary_key=True)
    rep_dashboard_id = Column(Integer, ForeignKey('lg_report_dashboard.id'))
    rep_dashgroup_id = Column(Integer, ForeignKey('lg_report_dashboard_group.id'))
    status = Column(Unicode)
    order = Column(Integer)


class LGReportDashroupToUser(Base):
    __tablename__ = 'lg_rep_dashboard_group_to_user'
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('lg_user.id'))
    rep_dashboard_group_id = Column(Integer, ForeignKey('lg_report_dashboard_group.id'))
    status = Column(Unicode)
    order = Column(Integer)


class LGDashGroupUserMap(Base):
    __tablename__ = 'lg_dshb_group_user'
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer)
    dshb_group_id = Column(Integer)
    status = Column(Unicode)
    order = Column(Integer)


class LGShowUserHomeDashboard(Base):
    __tablename__ = 'lg_show_user_home_dashboard'
    view_id = Column(Integer, primary_key=True)
    id = Column(Integer)
    user_id = Column(Integer)
    dashboard_id = Column(Integer)
    user_name = Column(Unicode)
    dtitle = Column(Unicode)
    description = Column(Unicode)
    status = Column(Unicode)


class CommunicationTemplates(Base):
    __tablename__ = 'Communication_Templates'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    description = Column(String)
    data = Column(JSONB)
    status = Column(Boolean)
    type = Column(String)
    has_params = Column(Boolean)


class Tasks(Base):
    __tablename__ = 'tasks'
    id = Column(Integer, primary_key=True)
    task_id = Column(UUID)
    task_type = Column(String)
    task_status = Column(String)
    arguments = Column(JSONB)
    create_datetime = Column(TIMESTAMP)
    lastchange_datetime = Column(TIMESTAMP)
    response = Column(JSONB)


class LgShowDashBoardDashgroups(Base):
    __tablename__ = 'lg_show_dashboard_dashgroups'
    viewid = Column(Integer, primary_key=True)
    username = Column(Unicode)
    dashboard_group_id = Column(Integer)
    dashboard_id = Column(Integer)
    dashboard_name = Column(Unicode)
    order = Column(Integer)
    dashboard_title = Column(Unicode)
    dashboard_file_name = Column(Unicode)
    dashgroup_name = Column(Unicode)
    dashgroup_description = Column(Unicode)
    dashgroup_display_name = Column(Unicode)


class LgShowReportDashBoardDashgroups(Base):
    __tablename__ = 'lg_show_report_dashboard_dashgroup'
    view_id = Column(Integer, primary_key=True)
    user_name = Column(Unicode)
    dashboard_group_id = Column(Integer)
    dashboard_id = Column(Integer)
    dashboard_name = Column(Unicode)
    order = Column(Integer)
    dashgroup_name = Column(Unicode)
    dashgroup_description = Column(Unicode)
    dashgroup_display_name = Column(Unicode)

# Only for Test Purpose
class Test3(Base):
    __tablename__ = 'test3'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    sex = Column(String)
    age = Column(Float)
    fare = Column(Float)
    cabin = Column(String)
    embarked = Column(String)
   
class SalesTest1(Base):
    __tablename__ = 'sales_test1'
    id = Column(Integer, primary_key=True)
    area_type = Column(String)
    availability = Column(String)
    location = Column(String)
    size = Column(String)
    society = Column(String)
    total_sqft = Column(String)
    bath = Column(Integer)
    balcony = Column(Integer)
    price = Column(Float)

class RetailTest1(Base):
    __tablename__ = 'retail_test1'
    id = Column(Integer, primary_key=True)
    country = Column(String)
    customerid = Column(Integer)
    description = Column(String)
    invoicedate = Column(String)
    invoiceno = Column(String)
    quantity = Column(Integer)
    stockcode = Column(String)
    unitprice = Column(Float)

class ORG_Test5(Base):
    __tablename__ = 'org_test5'
    id = Column(Integer, primary_key=True)
    activeind = Column(String)
    businessstartdate = Column(String)
    ceoname = Column(String)
    createdby = Column(String)
    createddate = Column(String)
    financialeffectivedate = Column(String)

    isx_id = Column(String)
    language_x_ref_id = Column(String)
    organisation_status_x_ref_id = Column(String)
    organisation_x_ref_id = Column(String)
    ownershiptypedate = Column(String)
    prefered_contacttype_id = Column(String)
    p_sector_sub_group_x_ref_id = Column(String)
    registeredname = Column(String)

    ownershiptype_x_ref_id = Column(String)
    registrationdate = Column(String)
    seta_x_ref_id = Column(String)
    shortname = Column(String)
    siccode_x_ref_id = Column(String)
    source_id = Column(String)

    statusdate = Column(String)
    sub_discipline_x_ref_id = Column(String)
    s_sector_sub_group_x_ref_id = Column(String)
    tradename = Column(String)
    translatedname = Column(String)
    updateby = Column(String)
    updateddate = Column(String)


def orm_to_dict(obj):
    return {col.name: getattr(obj, col.name) for col in obj.__table__.columns}
