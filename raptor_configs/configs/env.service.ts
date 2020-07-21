import { Injectable } from '@angular/core';

@Injectable({
  providedIn: 'root'
})
export class EnvService {

  // public host =  '192.168.0.162';
  // public service_host =  '192.168.0.162';
  public max_records = 100;
  public whhost = 'localhost';
  public dbhost = 'localhost';
  public service_host = 'localhost';
  public whport = 8082;
  public dbport = 5002;
  public wh_service_url =  'http://' + this.whhost + ':' + this.whport;
  public db_service_url =  'http://' + this.dbhost + ':' + this.dbport;

  public ddshost = 'localhost';
  public ddsport = 5005;
  public dds_service_url = 'http://' + this.ddshost + ':' + this.ddsport;

  // whether to run dds
  public dds_server_active  = true;
  public skip_spinner_for_services = ['/whapi/getTableColumns', '/get_all_tables_and_columns'];

  public io_host = 'localhost';
  public io_port = '3001';
  public io_namespace = 'test';
  public io_url = 'http://'+this.io_host+':'+this.io_port+'/'+this.io_namespace;
  public isAdmin = true;

  constructor() { }
}
