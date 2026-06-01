package main

import (
	"testing"
	"time"
)

func TestLoadMQTTPublisherConfigNormalizesBrokerAndTopics(t *testing.T) {
	t.Setenv("MQTT_ENABLED", "true")
	t.Setenv("MQTT_BROKER", "homeassistant.local:1883")
	t.Setenv("MQTT_CLIENT_ID", "")
	t.Setenv("MQTT_BASE_TOPIC", "/custom/base/")
	t.Setenv("MQTT_DISCOVERY_PREFIX", "/ha/")
	t.Setenv("MQTT_INTERVAL_SECONDS", "30")

	config := loadMQTTPublisherConfig()

	if !config.Enabled {
		t.Fatal("expected MQTT publisher to be enabled")
	}
	if config.Broker != "tcp://homeassistant.local:1883" {
		t.Fatalf("expected normalized broker, got %q", config.Broker)
	}
	if config.ClientID != defaultMQTTClientID {
		t.Fatalf("expected default client ID, got %q", config.ClientID)
	}
	if config.BaseTopic != "custom/base" {
		t.Fatalf("expected trimmed base topic, got %q", config.BaseTopic)
	}
	if config.DiscoveryPrefix != "ha" {
		t.Fatalf("expected trimmed discovery prefix, got %q", config.DiscoveryPrefix)
	}
	if config.Interval != time.Duration(minimumMQTTIntervalSeconds)*time.Second {
		t.Fatalf("expected minimum interval clamp, got %s", config.Interval)
	}
}

func TestMQTTSafeID(t *testing.T) {
	got := mqttSafeID("ViEventLog", "123/ABC", "room", "0", "temperature")
	want := "vieventlog_123_abc_room_0_temperature"
	if got != want {
		t.Fatalf("mqttSafeID() = %q, want %q", got, want)
	}
}

func TestBuildMQTTDiscoveryEntities(t *testing.T) {
	temp := 21.4
	humidity := 48.0
	heatSetpoint := 22.0
	room := Room{
		InstallationID:    "123",
		AccountID:         "user@example.com",
		GatewaySerial:     "gw-1",
		RoomID:            2,
		RoomName:          "Wohnzimmer",
		SystemName:        "Living Room",
		Temperature:       &temp,
		Humidity:          &humidity,
		HeatingSetpoint:   &heatSetpoint,
		TemperatureStatus: "connected",
	}
	config := mqttPublisherConfig{
		BaseTopic:       "vieventlog",
		DiscoveryPrefix: "homeassistant",
	}

	stateTopic := mqttRoomStateTopic(config, room)
	entities := buildMQTTDiscoveryEntities(config, room, stateTopic)

	if len(entities) != 3 {
		t.Fatalf("expected 3 discovery entities, got %d", len(entities))
	}
	if entities[0].ConfigTopic != "homeassistant/sensor/vieventlog_123_room_2_temperature/config" {
		t.Fatalf("unexpected temperature config topic %q", entities[0].ConfigTopic)
	}
	if entities[0].Config.StateTopic != "vieventlog/rooms/123/2/state" {
		t.Fatalf("unexpected state topic %q", entities[0].Config.StateTopic)
	}
	if entities[0].Config.ValueTemplate != "{{ value_json.temperature }}" {
		t.Fatalf("unexpected value template %q", entities[0].Config.ValueTemplate)
	}
	if entities[0].Config.Device.Identifiers[0] != "vieventlog_123_room_2" {
		t.Fatalf("unexpected device identifier %q", entities[0].Config.Device.Identifiers[0])
	}
}
