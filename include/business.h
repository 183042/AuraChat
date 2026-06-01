#pragma once
#include "context.h"
#include "protocol.h"
#include <string>
#include <memory>

void process_business(std::shared_ptr<ClientContext> ctx, PacketHeader hdr, std::string body);
