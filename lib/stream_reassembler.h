#pragma once

#include <mordor/fibersynchronization.h>
#include <boost/shared_ptr.hpp>
#include <queue>
#include <string>
#include <utility>

namespace lightning {

class StreamReassembler {
public:
    typedef boost::shared_ptr<StreamReassembler> ptr;

    StreamReassembler();

    void addChunk(uint64_t position,
                  boost::shared_ptr<std::string> data);

    void setEnd(uint64_t endPosition);

    boost::shared_ptr<std::string> nextChunk();
private:
    typedef std::pair<uint64_t, boost::shared_ptr<std::string> > Chunk;
    struct ChunkCompare {
        bool operator()(const Chunk& a, const Chunk& b) const {
            return a.first > b.first;
        }
    };

    typedef std::priority_queue<Chunk,
                                std::vector<Chunk>,
                                ChunkCompare>
            ChunkHeap;

    ChunkHeap chunks_;
    uint64_t readPosition_;
    uint64_t endPosition_;

    static const uint64_t kUnknownEndPosition = ~0ull;

    Mordor::FiberEvent nextChunkAvailable_;
    Mordor::FiberMutex mutex_;
};

}  // namespace lightning
