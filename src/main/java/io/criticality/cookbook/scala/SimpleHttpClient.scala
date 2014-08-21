package io.criticality.cookbook.scala

import java.io._
import org.apache.commons._
import org.apache.http._
import org.apache.http.client._
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.DefaultHttpClient

class SimpleHttpClient {

  def sendPost(url: String, paylod: String, contentType: ContentType.Value): Result = {

    val post = new HttpPost(url)

    val input = new StringEntity(paylod);
    input.setContentType(contentType.toString);
    post.setEntity(input);

    // send the post request
    val response = new DefaultHttpClient().execute(post)

    val br = new BufferedReader(new InputStreamReader((response.getEntity().getContent())));

    val buf = new StringBuffer
    var line: String = br.readLine
    while (line != null) {
      buf.append(line);
      line = br.readLine
    }

    Result(response.getStatusLine().getStatusCode(), buf.toString());

  }

  def authenticate(url: String, auth: Auth, formatableJSon: String): String = {
    sendPost(url, (formatableJSon format (auth.username, auth.password)), ContentType.JSON).response
  }

  case class Auth(username: String, password: String)

  case class Result(status: Int, response: String)

}

object ContentType extends Enumeration {
  type ContentType = Value
  val JSON = Value("application/json")
  val XML = Value("application/xml")
}