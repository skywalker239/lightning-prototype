#include "stream_reassembler.h"
#include <mordor/assert.h>
#include <mordor/log.h>

namespace lightning {

using Mordor::FiberEvent;
using Mordor::FiberMutex;
using Mordor::Log;
using Mordor::Logger;
using std::make_pair;
using std::string;

static Logger::ptr g_log = Log::lookup("lightning:stream_reassembler");

const uint64_t StreamReassembler::kUnknownEndPosition;

StreamReassembler::StreamReassembler()
    : readPosition_(0),
      endPosition_(kUnknownEndPosition),
      nextChunkAvailable_(false)
{}

void StreamReassembler::addChunk(uint64_t position,
                                 boost::shared_ptr<string> data)
{
    MORDOR_LOG_TRACE(g_log) << this << " addChunk(" << position <<
        ", " << data->length() << "), readPosition=" << readPosition_;
    FiberMutex::ScopedLock lk(mutex_);
    MORDOR_ASSERT(position >= readPosition_);
    chunks_.push(make_pair(position, data));
    if(position == readPosition_) {
        MORDOR_LOG_TRACE(g_log) << this << " next chunk now available at " <<
            readPosition_;
        nextChunkAvailable_.set();
    }
}

void StreamReassembler::setEnd(uint64_t endPosition) {
    FiberMutex::ScopedLock lk(mutex_);
    MORDOR_ASSERT(endPosition_ == kUnknownEndPosition);
    MORDOR_LOG_TRACE(g_log) << this << " setEnd(" << endPosition << ")";
    endPosition_ = endPosition;
}

boost::shared_ptr<string> StreamReassembler::nextChunk() {
    nextChunkAvailable_.wait();
    FiberMutex::ScopedLock lk(mutex_);

    if(chunks_.empty()) {
        MORDOR_ASSERT(endPosition_ != kUnknownEndPosition);
        MORDOR_ASSERT(readPosition_ == endPosition_);
        MORDOR_LOG_TRACE(g_log) << this << " stream ended";
        return boost::shared_ptr<string>();
    }

    const Chunk& top = chunks_.top();
    MORDOR_LOG_TRACE(g_log) << this << " got chunk(" <<
        top.first << ", " << top.second->length() << "), readPosition=" <<
        readPosition_;
    MORDOR_ASSERT(top.first == readPosition_);
    boost::shared_ptr<string> chunkData = top.second;
    readPosition_ += chunkData->length();
    chunks_.pop();

    if(chunks_.empty()) {
        if((endPosition_ != kUnknownEndPosition) &&
           (readPosition_ == endPosition_))
        {
            MORDOR_LOG_TRACE(g_log) << this <<
                " last chunk read";
        } else {
            MORDOR_LOG_TRACE(g_log) << this << " no chunks left";
            nextChunkAvailable_.reset();
        }
    } else {
        const Chunk& top = chunks_.top();
        if(top.first > readPosition_) {
            MORDOR_LOG_TRACE(g_log) << this << " readPosition=" <<
                readPosition_ << ", next chunk not yet available";
            nextChunkAvailable_.reset();
        }
    }
    return chunkData;
}

}  // namespace lightning
