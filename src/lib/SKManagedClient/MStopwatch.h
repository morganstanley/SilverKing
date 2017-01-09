#pragma once

#include "SKManagedClient.h"

using namespace System;

namespace SKManagedClient {

	public enum struct StopwatchState_M {
		SW_RUNNING = 0, SW_STOPPED
	};

	public interface class MStopwatch
	{
	public:

		// control
		virtual void start();
		virtual void stop();
		virtual void reset();

		// elapsed - The stopwatch Must be STOPPED when calling these methods!
		virtual Int64 getElapsedNanos();
		virtual Int64 getElapsedMillisLong();
		virtual int getElapsedMillis();
		virtual double getElapsedSeconds();

		// split
		virtual Int64 getSplitNanos();
		virtual Int64 getSplitMillisLong();
		virtual int getSplitMillis();
		virtual double getSplitSeconds();
		
		// misc.
		virtual String ^ getName();
		virtual StopwatchState_M getState();
		virtual String ^ toStringElapsed();
		virtual String ^ toStringSplit();

	};

}