#pragma once

namespace lightning {

template<typename T>
class Notifier {
public:
    virtual ~Notifier() {}

    virtual void notify(const T& t) = 0;
};

}  // namespace lightning
