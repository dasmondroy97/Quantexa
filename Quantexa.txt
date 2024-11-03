import scala.io.Source
import scala.util.{Try, Success, Failure}
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeFormatterBuilder
import java.time.temporal.ChronoField

// Define case classes to represent the data models with validation
case class Flight(passengerId: Int, flightId: Int, from: String, to: String, date: LocalDate)
case class Passenger(passengerId: Int, firstName: String, lastName: String)

object FlightAnalysis extends App {
  
  // Error handling type alias for better readability
  type ValidationResult[T] = Either[String, T]

  // Custom date formatter for "dd-MM-yy" format
  private val dateFormatter = new DateTimeFormatterBuilder()
    .appendPattern("dd-MM-yy")
    .parseDefaulting(ChronoField.YEAR_OF_ERA, 2000) // Handle two-digit years
    .toFormatter()

  // Function to load data from a CSV file with error handling
  def loadFromCSV[T](url: String, parser: Array[String] => ValidationResult[T]): List[T] = {
    try {
      // Read lines from the URL, dropping the header
      val lines = Source.fromURL(url).getLines().drop(1).toList 
      // Parse each line using the provided parser function
      val results = lines.map { line =>
        parser(line.split(",").map(_.trim))
      }
      
      // Separate successful results and errors
      val (errors, successes) = results.partitionMap(identity)
      
      // If there are any errors, log them
      if (errors.nonEmpty) {
        println(s"Warning: ${errors.size} records could not be parsed")
        println("First few errors:")
        errors.take(5).foreach(println)
      }
      
      // Return only the successful results
      successes
    } catch {
      case e: Exception => 
        // Log the error if loading data fails
        println(s"Error loading data from $url: ${e.getMessage}")
        List.empty[T] // Return an empty list on error
    }
  }

  // Improved parser for Passenger records with validation
  def parsePassenger(fields: Array[String]): ValidationResult[Passenger] = {
    Try {
      // Ensure there are exactly 3 fields and none are empty
      require(fields.length == 3, "Passenger record must have 3 fields")
      require(fields(0).nonEmpty && fields(1).nonEmpty && fields(2).nonEmpty, "All fields must be non-empty")
      // Create a Passenger object from the fields
      Passenger(fields(0).toInt, fields(1), fields(2))
    }.toEither.left.map(_.getMessage) // Convert to Either for error handling
  }

  // Improved parser for Flight records with validation
  def parseFlight(fields: Array[String]): ValidationResult[Flight] = {
    Try {
      // Ensure there are exactly 5 fields and none are empty
      require(fields.length == 5, "Flight record must have 5 fields")
      require(fields.forall(_.nonEmpty), "All fields must be non-empty")
      // Create a Flight object from the fields, parsing the date
      Flight(
        fields(0).toInt,
        fields(1).toInt,
        fields(2),
        fields(3),
        LocalDate.parse(fields(4), dateFormatter) // Use the custom date formatter
      )
    }.toEither.left.map(_.getMessage) // Convert to Either for error handling
  }

  // URLs for CSV files
  val flightsUrl = "https://raw.githubusercontent.com/dasmondroy97/Quantexa/46dfcd93aba3475305840595e19c4ca483cb8c7d/flightData.csv"
  val passengersUrl = "https://raw.githubusercontent.com/dasmondroy97/Quantexa/46dfcd93aba3475305840595e19c4ca483cb8c7d/passengers.csv"

  // Load data from the CSV files using the defined parsers
  val flights = loadFromCSV(flightsUrl, parseFlight)
  val passengers = loadFromCSV(passengersUrl, parsePassenger)

  // Object for performing analytics on flight data
  object Analytics {
    // Count flights per month
    def flightsPerMonth(): Map[Int, Int] = {
      flights.groupBy(_.date.getMonthValue) // Group flights by month
             .view.mapValues(_.size) // Count flights per month
             .toMap // Convert to a Map
    }

    // Find frequent flyers with their flight counts
    def frequentFlyers(): List[(Passenger, Int)] = {
      // Create a map of passengers for quick lookup
      val passengerMap = passengers.map(p => p.passengerId -> p).toMap
      
      // Group flights by passenger ID and count their flights
      flights.groupBy(_.passengerId)
            .map { case (id, flights) =>
              passengerMap.get(id) match {
                case Some(passenger) => (passenger, flights.size) // Associate passenger with flight count
                case None => throw new RuntimeException(s"Unknown passenger ID: $id") // Error if passenger not found
              }
            }
            .toList
            .sortBy(-_._2) // Sort by flight count in descending order
    }

    // Calculate the longest run of flights without a UK connection
    def longestRunWithoutUK(): List[(Int, Int)] = {
      def calculateRunForPassenger(passengerFlights: List[Flight]): Int = {
        // Sort flights by date to ensure chronological order
        val sortedFlights = passengerFlights.sortBy(_.date)
        
        // Create segments between UK appearances
        val segments = sortedFlights.foldLeft(List(List.empty[Flight])) { 
          case (acc @ (currentSegment :: rest), flight) =>
            // Start a new segment if the flight is to or from the UK
            if (flight.from == "UK" || flight.to == "UK") 
              List.empty[Flight] :: acc
            else 
              (flight :: currentSegment) :: rest // Continue the current segment
          case (acc, flight) => List(flight) :: acc // Add the first flight as a new segment
        }
        
        // Count unique countries in each segment
        segments.map { flights =>
          flights.flatMap(f => List(f.from, f.to)).toSet.size // Use Set to count unique locations
        }.maxOption.getOrElse(0) // Get the max unique count or 0 if no flights
      }

      // Process all passengers to calculate the longest run without UK
      flights.groupBy(_.passengerId)
            .view.mapValues(calculateRunForPassenger) // Apply calculation to each passenger
            .toList
            .filter(_._2 > 0) // Filter out zero runs
            .sortBy(-_._2) // Sort by longest runs in descending order
    }

    // Find pairs of passengers who flew together a minimum number of times
    def passengersFlyingTogether(minFlights: Int): List[((Int, Int), Int)] = {
      flights.groupBy(_.flightId) // Group flights by flight ID
            .values
            .flatMap { flightGroup =>
              // Create pairs of passengers who flew on the same flight
              for {
                f1 <- flightGroup
                f2 <- flightGroup
                if f1.passengerId < f2.passengerId // Avoid duplicates and self-pairs
              } yield ((f1.passengerId, f2.passengerId), 1) // Create a pair with a count of 1
            }
            .groupBy(_._1) // Group by passenger pairs
            .view.mapValues(_.size) // Count occurrences of each pair
            .filter(_._2 >= minFlights) // Filter by minimum flights
            .toList
            .sortBy(-_._2) // Sort by flight count in descending order
    }
  }

  // Object for pretty printing the results
  object Printer {
  def printFlightsPerMonth(): Unit = {
    println("\nFlights per Month:")
    println("Month | Flights")
    println("-" * 20)
    // Print each month's flight count in a formatted manner
    Analytics.flightsPerMonth().toList.sorted.foreach { case (month, count) =>
      println(f"$month%-6d | $count%7d") // Changed to display month as a number
    }
  }
  
    def printFrequentFlyers(): Unit = {
      println("\nFrequent Flyers:")
      println("Name | Flights")
      println("-" * 30)
      // Print the top 10 frequent flyers
      Analytics.frequentFlyers().take(10).foreach { case (passenger, count) =>
        println(f"${passenger.firstName} ${passenger.lastName}%-20s | $count%5d")
      }
    }

    def printLongestRun(): Unit = {
      println("\nLongest Run Without UK:")
      println("Passenger ID | Consecutive Countries")
      println("-" * 35)
      // Print the top 10 longest runs without a UK connection
      Analytics.longestRunWithoutUK().take(10).foreach { case (id, count) =>
        println(f"$id%11d | $count%19d")
      }
    }

    def printFlyingTogether(minFlights: Int): Unit = {
      println(s"\nPassengers Flying Together (>$minFlights flights):")
      println("Passenger 1 ID | Passenger 2 ID | Flights")
      println("-" * 55)
      // Print passenger pairs flying together more than specified times
      Analytics.passengersFlyingTogether(minFlights).foreach { case ((id1, id2), count) =>
        println(f"$id1%15d | $id2%15d | $count%7d")
      }
    }
  }

  // Print results
  Printer.printFlightsPerMonth()
  Printer.printFrequentFlyers()
  Printer.printLongestRun()
  Printer.printFlyingTogether(3) // Set minimum flights to filter
}