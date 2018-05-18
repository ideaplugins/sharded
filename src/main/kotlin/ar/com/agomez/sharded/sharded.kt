package ar.com.agomez.sharded

import java.io.File
import java.util.LinkedList
import java.util.concurrent.ThreadLocalRandom

/**
 * @author Alejandro Gomez
 */

typealias Record = Map<String, Any?>

typealias RecordFilter = (Record) -> Boolean

typealias RecordSorter = Comparator<Record>

typealias SortingFieldsExtractor = (Record) -> Record

fun List<LinkedList<*>>.anyNotEmpty() = any { it.peek() != null }

class Shard(private val id: String, var online: Boolean = true) {

    private val people = mutableListOf<Record>()

    private var lastResult = emptyList<Record>()

    fun printStatus() = println("Shard: $id, online: $online, record count: ${people.size}")

    fun save(person: Record) {
        people.add(person)
    }

    fun query(filter: RecordFilter, order: RecordSorter, transformer: SortingFieldsExtractor, limit: Int) = if (online) {
        people.filter(filter)
            .sortedWith(order)
            .take(limit)
            .also { lastResult = it }
            .map(transformer)
    } else {
        emptyList()
    }

    fun getLastResultWindow(window: ResultWindow) = lastResult.subList(window.skip, window.skip + window.keep)
}

class Coordinator(shardCount: Int, private val replicationFactor: Int) {

    init {
        require(shardCount >= replicationFactor)
        require(replicationFactor >= 1)
    }

    private val shards = List(shardCount) { Shard("Shard-Nr$it") }

    fun printStatus() {
        println("Shard count: ${shards.size}, replication factor: $replicationFactor")
        shards.forEach(Shard::printStatus)
    }

    fun killOneShard() {
        shards.forEach { it.online = true }
        shards[ThreadLocalRandom.current().nextInt(0, shards.size)].online = false
    }

    fun save(person: Record) {
        shards.shuffled()
            .take(replicationFactor)
            .forEach { it.save(person) }
    }

    fun query(page: Int, pageSize: Int, filter: RecordFilter, order: RecordSorter, transformer: SortingFieldsExtractor): List<Record> {
        val partials = shards.map { LinkedList(it.query(filter, order, transformer, (page + 1) * pageSize)) }
        val windows = combine(partials, page, pageSize, order)
        val shardResults = shards.zip(windows)
            .map { (shard, window) -> LinkedList(shard.getLastResultWindow(window)) }
        return combineResults(shardResults, page, pageSize, order)
    }

    private fun combine(results: List<LinkedList<Record>>, page: Int, pageSize: Int, order: RecordSorter) =
        SortingKeysMerger(results, order, page * pageSize, (page + 1) * pageSize).merge()

    private fun combineResults(shardResults: List<LinkedList<Record>>, page: Int, pageSize: Int, order: RecordSorter) =
        ResultsMerger(shardResults, order, page * pageSize).merge()
}

abstract class ChunksMerger<T>(protected val chunks: List<LinkedList<Record>>, private val sorter: RecordSorter) {

    protected abstract val result: T

    fun merge(): T {
        while (!done() && chunks.anyNotEmpty()) {
            val element = nextInOrder()
            update(element)
            chunks.filter { it.peek() == element }
                .forEach { it.poll() }
        }
        return result
    }

    private fun nextInOrder() = chunks
        .filter { it.peek() != null }
        .sortedWith(Comparator { o1, o2 -> sorter.compare(o1.peek(), o2.peek()) })[0]
        .peek()

    protected abstract fun done(): Boolean

    protected abstract fun update(element: Record)
}

class SortingKeysMerger(chunks: List<LinkedList<Record>>, sorter: RecordSorter, private val from: Int, private val upTo: Int): ChunksMerger<List<ResultWindow>>(chunks, sorter) {

    private var count = 0
    override val result = List(chunks.size) { ResultWindow() }

    override fun done() = count >= upTo

    override fun update(element: Record) {
        count++
        chunks.mapIndexed { index, it -> index to it }
            .filter { it.second.peek() == element }
            .forEach {
                result[it.first].apply {
                    if (count <= from) incSkip() else incKeep()
                }
            }
    }
}

class ResultsMerger(chunks: List<LinkedList<Record>>, sorter: RecordSorter, private val upTo: Int): ChunksMerger<List<Record>>(chunks, sorter) {

    override val result = mutableListOf<Record>()

    override fun done() = result.size >= upTo

    override fun update(element: Record) {
        result.add(element)
    }
}

class ResultWindow {
    var skip = 0
        private set(value) {
            field = value
        }

    var keep = 0
        private set(value) {
            field = value
        }

    fun incSkip() = ++skip

    fun incKeep() = ++keep
}

fun main(args: Array<String>) {
    val coordinator = Coordinator(7, 2)
    val people = readDataFile()
    people.shuffled().forEach(coordinator::save)
    val filter: RecordFilter = { person -> person["age"] as Int > 30 && person["gender"] != "MALE" }
    val transformer: SortingFieldsExtractor = { person -> person.filterKeys { it in listOf("id", "age") } }
    val order = Comparator.comparingInt { map: Record -> map["age"] as Int }.reversed()
        .thenComparingLong { map: Record -> map["id"] as Long }
    executeQuery(coordinator, people, filter, order, transformer, 5, 13)
    executeQuery(coordinator, people, { true }, Comparator.comparing { map: Record -> map["id"] as Long }, { map: Record -> map.filterKeys { it == "id" } }, 3, 10)
}

private fun executeQuery(coordinator: Coordinator, people: List<Record>, filter: RecordFilter, sorter: RecordSorter, transformer: SortingFieldsExtractor, page: Int, pageSize: Int) {
    coordinator.killOneShard()
    coordinator.printStatus()
    val result = coordinator.query(page, pageSize, filter, sorter, transformer)
    val expectedResult = people.filter(filter).sortedWith(sorter).drop(page * pageSize).take(pageSize)
    println("Result from shards")
    result.forEach(::println)
    println("Expected result")
    expectedResult.forEach(::println)
    check(result == expectedResult)
}

private fun readDataFile() =
    File(Coordinator::class.java.getResource("/data.csv").file)
        .useLines {
            it.drop(1)
                .map(::csvLineToRecord)
                .toList()
        }

private fun csvLineToRecord(line: String) =
    line.split(",")
        .map(String::trim)
        .toList()
        .let { mapOf("id" to it[0].toLong(), "firstName" to it[1], "lastName" to it[2], "age" to it[3].toInt(), "gender" to it[4]) }
