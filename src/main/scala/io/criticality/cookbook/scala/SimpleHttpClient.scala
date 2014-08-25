package io.criticality.cookbook.scala

import java.io._
import org.apache.commons._
import org.apache.http._
import org.apache.http.client._
import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.DefaultHttpClient
import java.util.ArrayList
import org.apache.http.message.BasicNameValuePair
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.entity.StringEntity
import scala.collection.immutable.HashMap
import org.apache.http.client.methods.HttpPut
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase

/**
 *
 * A simple http client that offers post and authentication for (primarily JSON) in scala
 *
 */
class SimpleHttpClient {

  def sendPost(url: String, payload: String, contentType: ContentType.Value, headers: Map[String, String]): Result = {

    val post = new HttpPost(url)
    send(payload, contentType, headers, post)

  }

  def sendPut(url: String, payload: String, contentType: ContentType.Value, headers: Map[String, String]): Result = {

    val put = new HttpPut(url)
    send(payload, contentType, headers, put)

  }

  private def send(payload: String, contentType: ContentType.Value, headers: Map[String, String], post: HttpEntityEnclosingRequestBase): Result = {
    headers.foreach {
      case (name, value) =>
        post.addHeader(name, value)
    }
    val input = new StringEntity(payload);
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
    sendPost(url, (formatableJSon format (auth.username, auth.password)), ContentType.JSON, new HashMap).response
  }

}

case class Auth(username: String, password: String)

case class Result(status: Int, response: String)

object ContentType extends Enumeration {
  type ContentType = Value
  val JSON = Value("application/json")
  val XML = Value("application/xml")
}