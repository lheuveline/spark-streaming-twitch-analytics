package TwitchStream

import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.json4s.jackson.{JsonMethods, Serialization}

object TFInterface {

    /*
    General function for annotating texts:
      - Call to TF Serving
      - Label using python fitted MultiLabelEncoder using embeded json
      - Filter predictions from model using difficulty threshold ( default 0.5 )
     */

    def make_tensorflow_serving_call(text:String): Map[String, List[Double]] = {

      // Make HTTP Call to TensorFlow Serving.
      // Need static IP for TFServing

      val containerIp = "172.25.0.2"
      val tfsPort = "8501"
      val api_endpoint = f"http://$containerIp:$tfsPort/v1/models/frwikimedia_use_50_cat:predict"
      val httpClient = HttpClients.createDefault()

      val postRequest = new HttpPost(api_endpoint)

      val data: Map[String, Array[String]] = Map(
        "instances" -> Array(text)
      )

      implicit val formats = org.json4s.DefaultFormats
      val jsonData = Serialization.write(data)
      val postData = new StringEntity(jsonData)
      postRequest.setEntity(postData)
      val resp = httpClient.execute(postRequest)
      val result = EntityUtils.toString(resp.getEntity)

      JsonMethods.parse(result).values.asInstanceOf[Map[String, List[Double]]]
    }

    def load_classes_labels(): Map[String, String] = {
      val src = scala.io.Source.fromResource("encoder_classes.json")
      JsonMethods.parse(src.reader()).values.asInstanceOf[Map[String, String]]
    }

    def formatPredictions(preds:List[Int], labels:Map[String, String]): List[Option[String]] = {

      /*

      Take list of filtered predictions and get labelName from index

       */

      // Values inverse sort
      val selectedLabels = preds.map(k => labels.get(k.toString))

      selectedLabels
    }

    def filterPredictions(preds:List[Double], threshold: Double): List[Int] = {
      val filteredPreds = preds.filter(x => x > threshold).map(x => preds.indexOf(x))
      filteredPreds
    }

    def getLabels(preds:List[Double]): List[Option[String]] = {

      // Get classes labels
      val labels = load_classes_labels()

      // filterPred
      val filteredPreds = filterPredictions(preds, threshold =  0.5)

      // format filtered preds
      val formattedPredictions = formatPredictions(filteredPreds, labels)
      formattedPredictions
    }

    def annotateText(text:String): List[Option[String]] = {
      /*
      Wrapper to annotate text using TFServing and decode labels

      TO-DO : move encoder classes to Redis at train / validation time
       */

      val tfs_response = make_tensorflow_serving_call(text)
      val preds = tfs_response("predictions").asInstanceOf[List[List[Double]]].flatten // Not working with multiple texts
      val filteredPreds = getLabels(preds)
      filteredPreds
    }
}
