package ca.mcit.bigdata.scala


import java.io.{BufferedWriter, FileWriter}
import scala.io.{BufferedSource, Source}

object ScalaReadFiles extends App {

  val source: BufferedSource = Source.fromFile("C:\\Users\\vamsh\\Downloads\\project-enrich-data\\data\\trips.txt")
  val lines: List[String] = source.getLines().drop(1).toList
  val trip: List[Trip] = lines.map(tripFromCsv)
  source.close()

  def tripFromCsv(csv: String): Trip = {
    val fields: Array[String] = csv.split(",", -1)
    Trip(fields(2), fields(1), fields(0), fields(3), if (fields(6) == "1") true else false)
  }

  val source2: BufferedSource = Source.fromFile("C:\\Users\\vamsh\\Downloads\\project-enrich-data\\data\\routes.txt")
  val lines2: List[String] = source2.getLines().drop(1).toList
  val route: List[Route] = lines2.map(routesFromCsv)
  val routeLookup = RouteLookup(route)
  source2.close()

  val tripRoute: List[TripRoute] = trip.map(trip => {
    val route: Route =
      routeLookup.lookup(trip.routeId.toInt)
    TripRoute(trip, route)
  })
  for (l <- tripRoute) {
    println(l)
  }

  def routesFromCsv(csv: String): Route = {
    val fields: Array[String] = csv.split(",", -1)
    Route(fields(0).toInt, fields(3), fields(6))
  }

  val calendarsource: BufferedSource = Source.fromFile("C:\\Users\\vamsh\\Downloads\\project-enrich-data\\data\\calendar.txt")
  val lines3: List[String] = calendarsource.getLines().drop(1).toList
  tripRoute.foreach(println)
  val calendar = lines3.map(calendarFromCsv)
  val calendarLookup = CalendarLookup(calendar)
  calendarsource.close()

  val enrichedTrip: List[EnrichedTrip] = {
    tripRoute.map(tripRoute => EnrichedTrip(tripRoute, calendarLookup.lookup(tripRoute.trip.serviceId)))
  }

  def calendarFromCsv(csv: String): Calendar = {
    val fields: Array[String] = csv.split(",", -1)
    Calendar(fields(0), fields(8), fields(9))
  }

  val calendar2 = CalendarLookup(calendar)
  val enrichedTrp = tripRoute.map(tripRoute => {
    val calendar = calendar2.lookup(tripRoute.trip.serviceId)
    EnrichedTrip(tripRoute, calendar)
  })
  for (elem <- enrichedTrip) {
    println(elem)
  }

  val writer: BufferedWriter = new BufferedWriter(new
      FileWriter("C:\\Users\\vamsh\\Downloads\\project-enrich-data\\csvfile"))
  writer.write(s"trip_id,service_id,route_id,trip_HeadSign,wheelchair_accessible,route_long_name," +
    s"route_color,date,exception_type\n")

  enrichedTrip.map {
    case EnrichedTrip(TripRoute(Trip(tripId, serviceId, routeId, tripHeadSign, wheelChairAccessible),
    Route(_, routeLongName, routeColor)), Calendar(_, date, exceptionType)) =>
      writer.write(s"$tripId,$serviceId,$routeId,$tripHeadSign,$wheelChairAccessible,$routeLongName," +
        s"$routeColor,$date,$exceptionType\n")
    case EnrichedTrip(TripRoute(Trip(tripId, _, _, _, _), Route(_, _, _)), _) => println(
      s"""Person: $tripId has no no calendar""".stripMargin)
  }
  writer.close()
  enrichedTrip.foreach(println)
}

case class Trip(tripId: String,
                serviceId: String,
                routeId: String,
                tripHeadSign: String,
                wheelChairAccessible: Boolean)

case class Route(routeId: Int, routeLongName: String, routeColor: String)

case class Calendar(serviceId: String, startDate: String, endDate: String)

case class TripRoute(trip: Trip, route: Route)

case class EnrichedTrip(tripRoute: TripRoute, calendar: Calendar)

case class RouteLookup(routes: List[Route]) {
  private val lookupTable: Map[Int, Route] =
    routes.map(route => route.routeId -> route).toMap

  def lookup(routeId: Int): Route =
    lookupTable.getOrElse(routeId, null)
}

case class CalendarLookup(calendar: List[Calendar]) {
  private val lookupTable: Map[String, Calendar] =
    calendar.map(calendar => calendar.serviceId -> calendar).toMap

  def lookup(serviceId: String): Calendar = lookupTable.getOrElse(serviceId, null)
}

