package io.github.phuongtran.androidoutbox

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.isActive

/**
 * Default coroutine-facing doorbell channel backed by [OutboxDoorbellReader].
 *
 * The native read is blocking, so the channel runs on [dispatcher]. Doorbells
 * are wake-up hints only; records must still be pulled through
 * [AndroidOutbox.readNextBatch].
 */
class BlockingOutboxDoorbellChannel(
    private val reader: OutboxDoorbellReader,
    private val dispatcher: CoroutineDispatcher = Dispatchers.IO,
) : OutboxDoorbellChannel {

    override fun events(): Flow<OutboxDoorbellEvent> {
        return flow {
            while (currentCoroutineContext().isActive) {
                val event = reader.readNextDoorbell() ?: break
                emit(event)
            }
        }.flowOn(dispatcher)
    }
}
