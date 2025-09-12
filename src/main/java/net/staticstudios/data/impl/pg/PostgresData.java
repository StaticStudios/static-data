package net.staticstudios.data.impl.pg;

import com.google.gson.annotations.SerializedName;

import java.util.Map;

public record PostgresData(@SerializedName("new") Map<String, String> newDataValueMap,
                           @SerializedName("old") Map<String, String> oldDataValueMap) {
}
