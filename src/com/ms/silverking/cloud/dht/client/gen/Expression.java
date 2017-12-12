package com.ms.silverking.cloud.dht.client.gen;

import com.ms.silverking.collection.Pair;

public interface Expression extends Statement {
	public Pair<Context,String> evaluate(Context context);
}
