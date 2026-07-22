package com.android.pt.outbox

import io.github.phuongtran.androidoutbox.OutboxDoorbellChannel
import io.github.phuongtran.androidoutbox.OutboxDoorbellEvent
import io.github.phuongtran.androidoutbox.OutboxDoorbellReader
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.isActive

class OutboxLabDoorbellChannel(
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
