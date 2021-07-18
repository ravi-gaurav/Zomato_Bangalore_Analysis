package requiredUDFs

import org.apache.spark.sql.functions.udf

object MenuListUDF {
  
//  UDF to convert elements of menu_item column from string to array
    def menu_list_udf() = {
      val menu_item_to_array_udf = (str:String) => {
        val requiredSubstr = str.substring(1, str.length()-1)
        val array_of_menu = requiredSubstr.split(",")
        array_of_menu
      }
      
      val menu_item_to_array = udf(menu_item_to_array_udf)
      menu_item_to_array
    }
}