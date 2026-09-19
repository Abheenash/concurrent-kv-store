#pragma once
#include <cstdio>
#include <cstdlib>
#define CHECK(c) do { if (!(c)) { std::fprintf(stderr, "CHECK failed: %s (%s:%d)\n", #c, __FILE__, __LINE__); std::exit(1); } } while (0)
#define CHECK_EQ(a, b) do { auto _a = (a); auto _b = (b); if (!(_a == _b)) { std::fprintf(stderr, "CHECK_EQ failed: %s == %s (%s:%d)\n", #a, #b, __FILE__, __LINE__); std::exit(1); } } while (0)
