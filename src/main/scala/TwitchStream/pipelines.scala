package TwitchStream

import com.johnsnowlabs.nlp.{DocumentAssembler, Finisher}
import com.johnsnowlabs.nlp.annotators.{StopWordsCleaner, Tokenizer}
import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline
import org.apache.spark.ml.{Pipeline, PipelineModel}

object pipelines {

  def getSentimentPipeline(): Pipeline = {

    val sentimentAnnotator:PipelineModel = PretrainedPipeline("analyze_sentiment", lang= "en").model // Only english model available

    val documentAssembler:DocumentAssembler = new DocumentAssembler()
      .setInputCol("text")
      .setOutputCol("document")

    val tokenizer:Tokenizer = new Tokenizer()
      .setInputCols("document")
      .setOutputCol("token")

    // Stopwords model : ! language hardcoded !
    val stopWordsCleaner:StopWordsCleaner = StopWordsCleaner.pretrained("stopwords_fr", lang = "fr")
      .setInputCols("token")
      .setOutputCol("clean_text")

    val finisher:Finisher = new Finisher()
      .setInputCols("clean_text", "sentiment")

    new Pipeline().setStages(Array(
      documentAssembler,
      tokenizer,
      stopWordsCleaner,
      sentimentAnnotator,
      finisher
    ))
  }
}
