package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	defaultMQTTClientID          = "vieventlog"
	defaultMQTTBaseTopic         = "vieventlog"
	defaultMQTTDiscoveryPrefix   = "homeassistant"
	defaultMQTTIntervalSeconds   = 300
	minimumMQTTIntervalSeconds   = 60
	mqttPublishTimeout           = 10 * time.Second
	mqttConnectTimeout           = 15 * time.Second
	mqttDiscoveryComponentSensor = "sensor"
)

type mqttPublisherConfig struct {
	Enabled         bool
	Broker          string
	Username        string
	Password        string
	ClientID        string
	BaseTopic       string
	DiscoveryPrefix string
	Interval        time.Duration
	Retain          bool
	Discovery       bool
}

type mqttRoomState struct {
	InstallationID    string   `json:"installation_id"`
	AccountID         string   `json:"account_id"`
	GatewaySerial     string   `json:"gateway_serial"`
	RoomID            int      `json:"room_id"`
	RoomName          string   `json:"room_name"`
	SystemName        string   `json:"system_name,omitempty"`
	RoomType          string   `json:"room_type,omitempty"`
	Temperature       *float64 `json:"temperature"`
	TemperatureStatus string   `json:"temperature_status,omitempty"`
	Humidity          *float64 `json:"humidity"`
	HumidityStatus    string   `json:"humidity_status,omitempty"`
	HeatingSetpoint   *float64 `json:"heating_setpoint"`
	CoolingSetpoint   *float64 `json:"cooling_setpoint"`
	UpdatedAt         string   `json:"updated_at"`
}

type mqttDiscoveryDevice struct {
	Identifiers  []string `json:"identifiers"`
	Name         string   `json:"name"`
	Manufacturer string   `json:"manufacturer,omitempty"`
	Model        string   `json:"model,omitempty"`
	ViaDevice    string   `json:"via_device,omitempty"`
}

type mqttDiscoveryConfig struct {
	Name                      string              `json:"name"`
	ObjectID                  string              `json:"object_id,omitempty"`
	UniqueID                  string              `json:"unique_id"`
	StateTopic                string              `json:"state_topic"`
	ValueTemplate             string              `json:"value_template"`
	AvailabilityTopic         string              `json:"availability_topic,omitempty"`
	PayloadAvailable          string              `json:"payload_available,omitempty"`
	PayloadNotAvailable       string              `json:"payload_not_available,omitempty"`
	DeviceClass               string              `json:"device_class,omitempty"`
	StateClass                string              `json:"state_class,omitempty"`
	UnitOfMeasurement         string              `json:"unit_of_measurement,omitempty"`
	SuggestedDisplayPrecision int                 `json:"suggested_display_precision,omitempty"`
	Device                    mqttDiscoveryDevice `json:"device"`
}

type mqttDiscoveryEntity struct {
	ConfigTopic string
	Config      mqttDiscoveryConfig
}

var (
	mqttPublisherMutex   sync.Mutex
	mqttPublisherRunning bool
	mqttPublisherStop    chan struct{}
	mqttPublisherTicker  *time.Ticker

	mqttPublishJobMutex sync.Mutex
	mqttPublishJobBusy  bool
)

type mqttClient struct {
	conn net.Conn
}

func loadMQTTPublisherConfig() mqttPublisherConfig {
	broker := strings.TrimSpace(os.Getenv("MQTT_BROKER"))
	if broker != "" && !strings.Contains(broker, "://") {
		broker = "tcp://" + broker
	}

	baseTopic := strings.Trim(getEnv("MQTT_BASE_TOPIC", defaultMQTTBaseTopic), "/")
	if baseTopic == "" {
		baseTopic = defaultMQTTBaseTopic
	}

	discoveryPrefix := strings.Trim(getEnv("MQTT_DISCOVERY_PREFIX", defaultMQTTDiscoveryPrefix), "/")
	if discoveryPrefix == "" {
		discoveryPrefix = defaultMQTTDiscoveryPrefix
	}

	clientID := strings.TrimSpace(getEnv("MQTT_CLIENT_ID", defaultMQTTClientID))
	if clientID == "" {
		clientID = defaultMQTTClientID
	}

	intervalSeconds := getEnvInt("MQTT_INTERVAL_SECONDS", defaultMQTTIntervalSeconds, minimumMQTTIntervalSeconds)

	return mqttPublisherConfig{
		Enabled:         getEnvBool("MQTT_ENABLED", false),
		Broker:          broker,
		Username:        os.Getenv("MQTT_USERNAME"),
		Password:        os.Getenv("MQTT_PASSWORD"),
		ClientID:        clientID,
		BaseTopic:       baseTopic,
		DiscoveryPrefix: discoveryPrefix,
		Interval:        time.Duration(intervalSeconds) * time.Second,
		Retain:          getEnvBool("MQTT_RETAIN", true),
		Discovery:       getEnvBool("MQTT_DISCOVERY", true),
	}
}

// StartMQTTPublisher starts the background MQTT publisher for RoomControl values.
func StartMQTTPublisher() error {
	config := loadMQTTPublisherConfig()
	if !config.Enabled {
		log.Println("MQTT publishing is disabled, publisher not started")
		return nil
	}

	if config.Broker == "" {
		return fmt.Errorf("MQTT_ENABLED=true but MQTT_BROKER is not set")
	}

	mqttPublisherMutex.Lock()
	defer mqttPublisherMutex.Unlock()

	if mqttPublisherRunning {
		log.Println("MQTT publisher already running")
		return nil
	}

	mqttPublisherStop = make(chan struct{})
	mqttPublisherTicker = time.NewTicker(config.Interval)
	mqttPublisherRunning = true

	log.Printf("MQTT room publisher started with interval %s, broker %s, base topic %s",
		config.Interval, config.Broker, config.BaseTopic)

	go func() {
		publishMQTTRoomsJob(config)

		for {
			select {
			case <-mqttPublisherTicker.C:
				publishMQTTRoomsJob(config)
			case <-mqttPublisherStop:
				log.Println("MQTT room publisher stopped")
				return
			}
		}
	}()

	return nil
}

// StopMQTTPublisher stops the background MQTT publisher and marks entities offline.
func StopMQTTPublisher() {
	mqttPublisherMutex.Lock()
	defer mqttPublisherMutex.Unlock()

	if !mqttPublisherRunning {
		return
	}

	if mqttPublisherTicker != nil {
		mqttPublisherTicker.Stop()
	}

	if mqttPublisherStop != nil {
		close(mqttPublisherStop)
	}

	config := loadMQTTPublisherConfig()
	if config.Enabled && config.Broker != "" {
		if err := publishMQTTOffline(config); err != nil {
			log.Printf("Failed to publish MQTT offline status: %v", err)
		}
	}

	mqttPublisherRunning = false
}

func connectMQTTClient(config mqttPublisherConfig) (*mqttClient, error) {
	address, err := mqttBrokerAddress(config.Broker)
	if err != nil {
		return nil, err
	}

	conn, err := net.DialTimeout("tcp", address, mqttConnectTimeout)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MQTT broker %s: %w", config.Broker, err)
	}

	client := &mqttClient{conn: conn}
	if err := client.Connect(config); err != nil {
		conn.Close()
		return nil, err
	}

	return client, nil
}

func mqttBrokerAddress(broker string) (string, error) {
	parsed, err := url.Parse(broker)
	if err != nil {
		return "", fmt.Errorf("invalid MQTT_BROKER %q: %w", broker, err)
	}

	switch parsed.Scheme {
	case "tcp", "mqtt":
	case "":
		parsed.Host = broker
	default:
		return "", fmt.Errorf("unsupported MQTT_BROKER scheme %q; use tcp://host:1883", parsed.Scheme)
	}

	host := parsed.Host
	if host == "" {
		host = parsed.Path
	}
	if host == "" {
		return "", fmt.Errorf("invalid MQTT_BROKER %q", broker)
	}
	if !strings.Contains(host, ":") {
		host += ":1883"
	}
	return host, nil
}

func (c *mqttClient) Connect(config mqttPublisherConfig) error {
	var variableHeader []byte
	protocolName, err := mqttEncodeString("MQTT")
	if err != nil {
		return err
	}
	variableHeader = append(variableHeader, protocolName...)
	variableHeader = append(variableHeader, 4)

	connectFlags := byte(0x02) // clean session
	if config.Username != "" {
		connectFlags |= 0x80
		if config.Password != "" {
			connectFlags |= 0x40
		}
	}
	variableHeader = append(variableHeader, connectFlags)
	variableHeader = binary.BigEndian.AppendUint16(variableHeader, 60)

	payload, err := mqttEncodeString(config.ClientID)
	if err != nil {
		return err
	}
	if config.Username != "" {
		username, err := mqttEncodeString(config.Username)
		if err != nil {
			return err
		}
		payload = append(payload, username...)

		if config.Password != "" {
			password, err := mqttEncodeString(config.Password)
			if err != nil {
				return err
			}
			payload = append(payload, password...)
		}
	}

	packet := append(variableHeader, payload...)
	if err := c.writePacket(0x10, packet); err != nil {
		return err
	}

	if err := c.conn.SetReadDeadline(time.Now().Add(mqttConnectTimeout)); err != nil {
		return err
	}
	defer c.conn.SetReadDeadline(time.Time{})

	header := make([]byte, 4)
	if _, err := io.ReadFull(c.conn, header); err != nil {
		return fmt.Errorf("failed to read MQTT CONNACK: %w", err)
	}
	if header[0] != 0x20 || header[1] != 0x02 {
		return fmt.Errorf("unexpected MQTT CONNACK header: %x", header[:2])
	}
	if header[3] != 0 {
		return fmt.Errorf("MQTT broker rejected connection with code %d", header[3])
	}

	return nil
}

func (c *mqttClient) Publish(topic string, payload []byte, retain bool) error {
	topicBytes, err := mqttEncodeString(topic)
	if err != nil {
		return err
	}

	packet := append(topicBytes, payload...)
	header := byte(0x30)
	if retain {
		header |= 0x01
	}
	return c.writePacket(header, packet)
}

func (c *mqttClient) Disconnect() {
	_ = c.writePacket(0xE0, nil)
	_ = c.conn.Close()
}

func (c *mqttClient) writePacket(header byte, payload []byte) error {
	packet := []byte{header}
	packet = append(packet, mqttEncodeRemainingLength(len(payload))...)
	packet = append(packet, payload...)

	if err := c.conn.SetWriteDeadline(time.Now().Add(mqttPublishTimeout)); err != nil {
		return err
	}
	defer c.conn.SetWriteDeadline(time.Time{})

	_, err := c.conn.Write(packet)
	return err
}

func mqttEncodeString(value string) ([]byte, error) {
	if len(value) > 65535 {
		return nil, fmt.Errorf("MQTT string exceeds 65535 bytes")
	}
	result := make([]byte, 2, 2+len(value))
	binary.BigEndian.PutUint16(result, uint16(len(value)))
	result = append(result, []byte(value)...)
	return result, nil
}

func mqttEncodeRemainingLength(length int) []byte {
	var encoded []byte
	for {
		digit := byte(length % 128)
		length /= 128
		if length > 0 {
			digit |= 128
		}
		encoded = append(encoded, digit)
		if length == 0 {
			break
		}
	}
	return encoded
}

func publishMQTTRoomsJob(config mqttPublisherConfig) {
	mqttPublishJobMutex.Lock()
	if mqttPublishJobBusy {
		log.Println("MQTT room publishing job already running, skipping this tick")
		mqttPublishJobMutex.Unlock()
		return
	}
	mqttPublishJobBusy = true
	mqttPublishJobMutex.Unlock()

	defer func() {
		mqttPublishJobMutex.Lock()
		mqttPublishJobBusy = false
		mqttPublishJobMutex.Unlock()
	}()

	client, err := connectMQTTClient(config)
	if err != nil {
		log.Printf("MQTT publisher could not connect to broker: %v", err)
		return
	}
	defer client.Disconnect()

	if err := publishMQTTAvailability(client, config, "online"); err != nil {
		log.Printf("Failed to publish MQTT online status: %v", err)
	}

	activeAccounts, err := GetActiveAccounts()
	if err != nil {
		log.Printf("MQTT publisher could not load active accounts: %v", err)
		return
	}

	if len(activeAccounts) == 0 {
		log.Println("MQTT publisher found no active accounts")
		return
	}

	publishedRooms := 0
	publishedEntities := 0
	cacheDuration := mqttFeatureCacheDuration(config.Interval)

	for _, account := range activeAccounts {
		token, err := ensureAccountAuthenticated(account)
		if err != nil {
			log.Printf("MQTT publisher failed to authenticate account %s: %v", account.Email, err)
			continue
		}

		for _, installationID := range token.InstallationIDs {
			installation, ok := token.Installations[installationID]
			if !ok {
				log.Printf("MQTT publisher installation %s not found in token cache", installationID)
				continue
			}

			rooms := collectRoomsForMQTT(account, token, installation, cacheDuration)
			for _, room := range rooms {
				entityCount, err := publishMQTTRoom(client, config, room)
				if err != nil {
					log.Printf("MQTT publisher failed for room %s (%s/%d): %v",
						mqttRoomDisplayName(room), room.InstallationID, room.RoomID, err)
					continue
				}
				publishedRooms++
				publishedEntities += entityCount
			}
		}
	}

	log.Printf("MQTT room publishing job completed. Rooms: %d, discovery entities: %d",
		publishedRooms, publishedEntities)
}

func collectRoomsForMQTT(account *Account, token *AccountToken, installation *Installation, cacheDuration time.Duration) []Room {
	var rooms []Room

	for _, gateway := range installation.Gateways {
		for _, device := range gateway.Devices {
			if device.DeviceType != "roomControl" {
				continue
			}

			if !checkAPIRateLimit() {
				log.Println("MQTT publisher API rate limit reached, skipping remaining RoomControl devices")
				return rooms
			}

			features, err := fetchFeaturesWithCustomCache(installation.ID, gateway.Serial, device.DeviceID, token.AccessToken, cacheDuration)
			if err != nil {
				log.Printf("MQTT publisher failed to fetch RoomControl features for device %s: %v", device.DeviceID, err)
				continue
			}

			deviceRooms := extractRoomData(installation.ID, account.ID, gateway.Serial, features.RawFeatures)
			applyAccountRoomSettings(account, installation.ID, deviceRooms)
			rooms = append(rooms, deviceRooms...)
		}
	}

	return rooms
}

func applyAccountRoomSettings(account *Account, installationID string, rooms []Room) {
	if account.RoomSettings == nil {
		return
	}

	for i := range rooms {
		roomKey := fmt.Sprintf("%s:%d", installationID, rooms[i].RoomID)
		if settings, ok := account.RoomSettings[roomKey]; ok && settings.Name != "" {
			rooms[i].RoomName = settings.Name
		}
	}
}

func publishMQTTRoom(client *mqttClient, config mqttPublisherConfig, room Room) (int, error) {
	if room.Temperature == nil && room.Humidity == nil &&
		room.HeatingSetpoint == nil && room.CoolingSetpoint == nil {
		return 0, nil
	}

	stateTopic := mqttRoomStateTopic(config, room)
	entities := buildMQTTDiscoveryEntities(config, room, stateTopic)

	if config.Discovery {
		for _, entity := range entities {
			if err := publishMQTTJSON(client, entity.ConfigTopic, entity.Config, true); err != nil {
				return 0, err
			}
		}
	}

	state := mqttRoomState{
		InstallationID:    room.InstallationID,
		AccountID:         room.AccountID,
		GatewaySerial:     room.GatewaySerial,
		RoomID:            room.RoomID,
		RoomName:          mqttRoomDisplayName(room),
		SystemName:        room.SystemName,
		RoomType:          room.RoomType,
		Temperature:       room.Temperature,
		TemperatureStatus: room.TemperatureStatus,
		Humidity:          room.Humidity,
		HumidityStatus:    room.HumidityStatus,
		HeatingSetpoint:   room.HeatingSetpoint,
		CoolingSetpoint:   room.CoolingSetpoint,
		UpdatedAt:         time.Now().UTC().Format(time.RFC3339),
	}

	if err := publishMQTTJSON(client, stateTopic, state, config.Retain); err != nil {
		return 0, err
	}

	if config.Discovery {
		return len(entities), nil
	}
	return 0, nil
}

func buildMQTTDiscoveryEntities(config mqttPublisherConfig, room Room, stateTopic string) []mqttDiscoveryEntity {
	displayName := mqttRoomDisplayName(room)
	deviceID := mqttRoomDeviceID(room)
	device := mqttDiscoveryDevice{
		Identifiers:  []string{deviceID},
		Name:         displayName,
		Manufacturer: "Viessmann",
		Model:        "SmartClimate RoomControl",
	}

	var entities []mqttDiscoveryEntity
	precision := 1

	if room.Temperature != nil {
		entities = append(entities, mqttDiscoveryEntity{
			ConfigTopic: mqttDiscoveryTopic(config, room, "temperature"),
			Config: mqttDiscoveryConfig{
				Name:                      "Temperature",
				ObjectID:                  mqttRoomEntityID(room, "temperature"),
				UniqueID:                  mqttRoomEntityID(room, "temperature"),
				StateTopic:                stateTopic,
				ValueTemplate:             "{{ value_json.temperature }}",
				AvailabilityTopic:         mqttAvailabilityTopic(config),
				PayloadAvailable:          "online",
				PayloadNotAvailable:       "offline",
				DeviceClass:               "temperature",
				StateClass:                "measurement",
				UnitOfMeasurement:         "\u00b0C",
				SuggestedDisplayPrecision: precision,
				Device:                    device,
			},
		})
	}

	if room.Humidity != nil {
		entities = append(entities, mqttDiscoveryEntity{
			ConfigTopic: mqttDiscoveryTopic(config, room, "humidity"),
			Config: mqttDiscoveryConfig{
				Name:                "Humidity",
				ObjectID:            mqttRoomEntityID(room, "humidity"),
				UniqueID:            mqttRoomEntityID(room, "humidity"),
				StateTopic:          stateTopic,
				ValueTemplate:       "{{ value_json.humidity }}",
				AvailabilityTopic:   mqttAvailabilityTopic(config),
				PayloadAvailable:    "online",
				PayloadNotAvailable: "offline",
				DeviceClass:         "humidity",
				StateClass:          "measurement",
				UnitOfMeasurement:   "%",
				Device:              device,
			},
		})
	}

	if room.HeatingSetpoint != nil {
		entities = append(entities, mqttDiscoveryEntity{
			ConfigTopic: mqttDiscoveryTopic(config, room, "heating_setpoint"),
			Config: mqttDiscoveryConfig{
				Name:                      "Heating Setpoint",
				ObjectID:                  mqttRoomEntityID(room, "heating_setpoint"),
				UniqueID:                  mqttRoomEntityID(room, "heating_setpoint"),
				StateTopic:                stateTopic,
				ValueTemplate:             "{{ value_json.heating_setpoint }}",
				AvailabilityTopic:         mqttAvailabilityTopic(config),
				PayloadAvailable:          "online",
				PayloadNotAvailable:       "offline",
				DeviceClass:               "temperature",
				StateClass:                "measurement",
				UnitOfMeasurement:         "\u00b0C",
				SuggestedDisplayPrecision: precision,
				Device:                    device,
			},
		})
	}

	if room.CoolingSetpoint != nil {
		entities = append(entities, mqttDiscoveryEntity{
			ConfigTopic: mqttDiscoveryTopic(config, room, "cooling_setpoint"),
			Config: mqttDiscoveryConfig{
				Name:                      "Cooling Setpoint",
				ObjectID:                  mqttRoomEntityID(room, "cooling_setpoint"),
				UniqueID:                  mqttRoomEntityID(room, "cooling_setpoint"),
				StateTopic:                stateTopic,
				ValueTemplate:             "{{ value_json.cooling_setpoint }}",
				AvailabilityTopic:         mqttAvailabilityTopic(config),
				PayloadAvailable:          "online",
				PayloadNotAvailable:       "offline",
				DeviceClass:               "temperature",
				StateClass:                "measurement",
				UnitOfMeasurement:         "\u00b0C",
				SuggestedDisplayPrecision: precision,
				Device:                    device,
			},
		})
	}

	return entities
}

func publishMQTTOffline(config mqttPublisherConfig) error {
	client, err := connectMQTTClient(config)
	if err != nil {
		return err
	}
	defer client.Disconnect()

	return publishMQTTAvailability(client, config, "offline")
}

func publishMQTTAvailability(client *mqttClient, config mqttPublisherConfig, value string) error {
	return publishMQTTRaw(client, mqttAvailabilityTopic(config), []byte(value), config.Retain)
}

func publishMQTTJSON(client *mqttClient, topic string, payload interface{}, retain bool) error {
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return publishMQTTRaw(client, topic, payloadJSON, retain)
}

func publishMQTTRaw(client *mqttClient, topic string, payload []byte, retain bool) error {
	return client.Publish(topic, payload, retain)
}

func mqttFeatureCacheDuration(interval time.Duration) time.Duration {
	cacheDuration := interval - 5*time.Second
	if cacheDuration < 10*time.Second {
		return 10 * time.Second
	}
	return cacheDuration
}

func mqttAvailabilityTopic(config mqttPublisherConfig) string {
	return config.BaseTopic + "/status"
}

func mqttRoomStateTopic(config mqttPublisherConfig, room Room) string {
	return fmt.Sprintf("%s/rooms/%s/%d/state",
		config.BaseTopic,
		mqttSafeTopicPart(room.InstallationID),
		room.RoomID)
}

func mqttDiscoveryTopic(config mqttPublisherConfig, room Room, suffix string) string {
	return fmt.Sprintf("%s/%s/%s/config",
		config.DiscoveryPrefix,
		mqttDiscoveryComponentSensor,
		mqttRoomEntityID(room, suffix))
}

func mqttRoomEntityID(room Room, suffix string) string {
	return mqttSafeID("vieventlog", room.InstallationID, "room", strconv.Itoa(room.RoomID), suffix)
}

func mqttRoomDeviceID(room Room) string {
	return mqttSafeID("vieventlog", room.InstallationID, "room", strconv.Itoa(room.RoomID))
}

func mqttRoomDisplayName(room Room) string {
	defaultName := fmt.Sprintf("Raum %d", room.RoomID)
	if room.RoomName != "" && room.RoomName != defaultName {
		return room.RoomName
	}
	if room.SystemName != "" {
		return room.SystemName
	}
	if room.RoomName != "" {
		return room.RoomName
	}
	return defaultName
}

func mqttSafeTopicPart(value string) string {
	return mqttSafeID(value)
}

func mqttSafeID(parts ...string) string {
	raw := strings.ToLower(strings.Join(parts, "_"))
	var b strings.Builder
	lastUnderscore := false

	for _, r := range raw {
		isLetter := r >= 'a' && r <= 'z'
		isDigit := r >= '0' && r <= '9'
		if isLetter || isDigit {
			b.WriteRune(r)
			lastUnderscore = false
			continue
		}

		if !lastUnderscore {
			b.WriteByte('_')
			lastUnderscore = true
		}
	}

	safe := strings.Trim(b.String(), "_")
	if safe == "" {
		return "unknown"
	}
	return safe
}

func getEnvBool(key string, defaultValue bool) bool {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return defaultValue
	}

	switch strings.ToLower(value) {
	case "1", "true", "yes", "y", "on", "enabled":
		return true
	case "0", "false", "no", "n", "off", "disabled":
		return false
	default:
		log.Printf("Invalid boolean value for %s=%q, using default %t", key, value, defaultValue)
		return defaultValue
	}
}

func getEnvInt(key string, defaultValue, minimumValue int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return defaultValue
	}

	parsed, err := strconv.Atoi(value)
	if err != nil {
		log.Printf("Invalid integer value for %s=%q, using default %d", key, value, defaultValue)
		return defaultValue
	}

	if parsed < minimumValue {
		log.Printf("%s=%d is below minimum %d, using %d", key, parsed, minimumValue, minimumValue)
		return minimumValue
	}

	return parsed
}
