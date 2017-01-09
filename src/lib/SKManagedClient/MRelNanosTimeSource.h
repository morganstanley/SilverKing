#pragma once

using namespace System;

namespace SKManagedClient {

	public interface class MRelNanosTimeSource
	{
	public:
		/**
		* @return a relative time in nanoseconds
		*/
		virtual Int64 relTimeNanos();

	};

}