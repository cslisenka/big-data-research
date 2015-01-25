
package org.scalaby

// import io.Source


object Crawler {

  // def crawl(): Int = {


  // }


  // def getLinks(html: String): Set[String] =
  //   linkRegex.findAllMatchIn(html).map(_.toString.replace("\"", "")).toSet


  // def getHttp(url: String) = {
  //   val in = Source.fromURL(domain + url, "utf8")
  //   val response = in.getLines.mkString
  //   in.close()
  //   response
  // }

}


// val domain = "http://en.wikipedia.org"
// val startPage = "/wiki/Main_Page"
// val linkRegex = """\"/wiki/[a-zA-Z\-_]+\"""".r




// val links = getLinks(getHttp(startPage))
// links.foreach(println)
// println(links.size)

// val allLinks = time(links.par.flatMap(link => getLinks(getHttp(link))))
// println(allLinks.size)
