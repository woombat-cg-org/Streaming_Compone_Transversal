import scala.util.parsing.json.JSON

object StreamingInit {
    def configInit():(String, String, String, String, String, String, String, String, String, String, String, String, String, String, List[String], String, String, String)={
        var jsonFile: String = "config.json"    
        var jsonContent: String = scala.io.Source.fromFile(jsonFile).mkString
        var json = JSON.parseFull(jsonContent)
        var config = json.get.asInstanceOf[Map[String,Any]]
        ////////////////////////===================////////////////////////////////////////
        val log_level = config.get("log_level").get.asInstanceOf[String]
        val spark_master = config.get("spark_master").get.asInstanceOf[String]
        val path_maestra = config.get("path_maestra").get.asInstanceOf[String]
        val path_estructura = config.get("path_estructura").get.asInstanceOf[String]
        val path_batch = config.get("path_batch").get.asInstanceOf[String]
        val kudu_master = config.get("kudu_master").get.asInstanceOf[String]
        val ss_batch_secs = config.get("ss_batch_secs").get.asInstanceOf[String]
        val spark_log_level = config.get("spark_log_level").get.asInstanceOf[String]
        val brokers = config.get("brokers").get.asInstanceOf[String]
        val group_id = config.get("group_id").get.asInstanceOf[String]
        val security = config.get("security").get.asInstanceOf[String]
        val sasl_mechanism = config.get("sasl_mechanism").get.asInstanceOf[String]
        val identificador = config.get("identificador").get.asInstanceOf[String]
        val capacidad = config.get("capacidad").get.asInstanceOf[String]
        val list_name_div = config.get("list_name_div").get.asInstanceOf[List[String]]
        val col_name_div = config.get("col_name_div").get.asInstanceOf[String]
        val time_zone = config.get("time_zone").get.asInstanceOf[String]
        val usuario_kerberos = config.get("usuario_kerberos").get.asInstanceOf[String]
        return (log_level, spark_master, path_maestra, path_estructura, path_batch, kudu_master, ss_batch_secs, spark_log_level, brokers, group_id, security, sasl_mechanism, identificador, capacidad, list_name_div, col_name_div,time_zone, usuario_kerberos)
    }
}

  