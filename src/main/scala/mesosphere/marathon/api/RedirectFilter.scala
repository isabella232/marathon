package mesosphere.marathon.api

import scala.collection.JavaConverters._
import com.google.inject.Inject
import java.net.{HttpURLConnection, URL}
import java.io.{OutputStreamWriter, InputStream}
import java.util.concurrent.atomic.AtomicBoolean
import java.util.logging.Logger
import javax.inject.Named
import javax.servlet._
import javax.servlet.http.{HttpServletResponse, HttpServletRequest}
import mesosphere.marathon.{MarathonSchedulerService, ModuleNames}


class RedirectFilter @Inject()
    (schedulerService: MarathonSchedulerService,
     @Named(ModuleNames.NAMED_LEADER_ATOMIC_BOOLEAN) leader: AtomicBoolean)
  extends Filter  {

  val log = Logger.getLogger(getClass.getName)

  def init(filterConfig: FilterConfig) {}

  def copy(input: InputStream, output: OutputStreamWriter) = {
    val bytes = new Array[Byte](1024)
    Iterator
      .continually(input.read(bytes))
      .takeWhile(-1 !=)
      .foreach(read => output.write(new String(bytes, 0, read), 0, read))
  }

  def doFilter(rawRequest: ServletRequest,
               rawResponse: ServletResponse,
               chain: FilterChain) {
    try {
      if (rawRequest.isInstanceOf[HttpServletRequest]) {
        val request = rawRequest.asInstanceOf[HttpServletRequest]
        val leaderData = schedulerService.getLeader
        val response = rawResponse.asInstanceOf[HttpServletResponse]

        if (schedulerService.isLeader) {
          chain.doFilter(request, response)
        } else {
          log.info("Proxying request.")

          val method = request.getMethod

          val proxy =
            new URL("http://%s%s".format(leaderData.get, request.getRequestURI))
              .openConnection().asInstanceOf[HttpURLConnection]


          val names = request.getHeaderNames
          // getHeaderNames() and getHeaders() are known to return null, see:
          // http://docs.oracle.com/javaee/6/api/javax/servlet/http/HttpServletRequest.html#getHeaders(java.lang.String)
          if (names != null) {
            for (name <- names.asScala) {
              val values = request.getHeaders(name)
              if (values != null) {
                proxy.setRequestProperty(name, values.asScala.mkString(","))
              }
            }
          }
          proxy.setDoOutput(method == "POST")
          proxy.setRequestMethod(method)

          method match {
            case "POST" =>
              val proxyWriter =
                new OutputStreamWriter(proxy.getOutputStream)
              val input = request.getInputStream
              copy(input, proxyWriter)
              proxyWriter.close
            case _ =>
          }

          val proxyReader = proxy.getInputStream

          response.setStatus(proxy.getResponseCode())

          val responseWriter =
            new OutputStreamWriter(response.getOutputStream)

          copy(proxyReader, responseWriter)
          proxyReader.close
          responseWriter.close
        }
      }
    } catch {
      case t: Throwable => log.warning("Exception while proxying: " + t)
    }
  }

  def destroy() {
    //NO-OP
  }
}
