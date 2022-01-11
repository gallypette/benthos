package processor

import (
	"fmt"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/gabs/v2"
	"net"
	"time"

	"github.com/Jeffail/benthos/v3/internal/tracing"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/oschwald/geoip2-golang"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeGeoIP] = TypeSpec{
		constructor: NewGeoIP,
		Description: `
		// todo some docs
		`,
	}
}

//------------------------------------------------------------------------------

// GeoIPConfig contains configuration fields for the GeoIP processor.
type GeoIPConfig struct {
	Parts        []int  `json:"parts" yaml:"parts"`
	DatabaseType string `json:"database_type" yaml:"database_type"`
	Database     string `json:"database" yaml:"database"`
	Field        string `json:"field" yaml:"field"`
}

// GeoCoord contains the city coordinates returned by geoIP
type GeoCoord struct {
	geoip_lat     float64 `json:"geoip_lat"`
	geoip_lon     float64 `json:"geoip_lon"`
	geoip_country string  `json:"geoip_country"`
	geoip_city    string  `json:"geoip_city"`
}

// NewGeoIPConfig returns a GeoIPConfig with default values.
func NewGeoIPConfig() GeoIPConfig {
	return GeoIPConfig{
		Parts:        []int{},
		DatabaseType: "",
		Database:     "",
		Field:        "",
	}
}

//------------------------------------------------------------------------------

type geoipReader func(db *geoip2.Reader, ip net.IP) (interface{}, error)

func cityReader() geoipReader {
	return func(db *geoip2.Reader, ip net.IP) (interface{}, error) {
		res, err := db.City(ip)
		if err != nil {
			return nil, err
		}
		ret := GeoCoord{
			geoip_lat:     res.Location.Latitude,
			geoip_lon:     res.Location.Longitude,
			geoip_country: res.Country.Names["en"],
			geoip_city:    res.City.Names["en"],
		}
		return ret, nil
	}
}

func asnReader() geoipReader {
	return func(db *geoip2.Reader, ip net.IP) (interface{}, error) {
		res, err := db.ASN(ip)
		if err != nil {
			return nil, err
		}
		return res, nil
	}
}

func getReader(dbtype string) (geoipReader, error) {
	switch dbtype {
	case "asn":
		return asnReader(), nil
	case "city":
		return cityReader(), nil
	}
	return nil, fmt.Errorf("unknown database type: %s", dbtype)

}

//------------------------------------------------------------------------------

// GeoIP is a processor that looks up IP addresses.
type GeoIP struct {
	parts   []int
	handler *geoip2.Reader
	reader  geoipReader

	conf  Config
	log   log.Modular
	stats metrics.Type

	mCount     metrics.StatCounter
	mErr       metrics.StatCounter
	mSent      metrics.StatCounter
	mBatchSent metrics.StatCounter
}

// NewGeoIP returns a GeoIP processor.
func NewGeoIP(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	g := &GeoIP{
		parts: conf.GeoIP.Parts,
		conf:  conf,
		log:   log,
		stats: stats,

		mCount:     stats.GetCounter("count"),
		mErr:       stats.GetCounter("error"),
		mSent:      stats.GetCounter("sent"),
		mBatchSent: stats.GetCounter("batch.sent"),
	}

	var err error
	g.handler, err = geoip2.Open(g.conf.GeoIP.Database)
	if err != nil {
		g.log.Debugf("Failed to open geoip database: %v\n", err)
		return nil, err
	}
	if g.reader, err = getReader(g.conf.GeoIP.DatabaseType); err != nil {
		return nil, err
	}

	return g, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (g *GeoIP) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	g.mCount.Incr(1)
	newMsg := msg.Copy()

	proc := func(index int, span *tracing.Span, part types.Part) error {

		ipj, err := part.JSON()
		if err != nil {
			g.mErr.Incr(1)
			g.log.Debugf("Failed to parse JSON ip field: %v\n", err)
			return fmt.Errorf("Failed to parse JSON ip field: %v\n", err)
		}

		gPart := gabs.Wrap(ipj)
		var ip []byte
		valueip, ok := gPart.Path(g.conf.GeoIP.Field).Data().(string)
		if ok {
			ip = net.ParseIP(valueip)
			if ip == nil {
				g.mErr.Incr(1)
				g.log.Debugf("Failed to parse as an IP: %v\n", valueip)
				return fmt.Errorf("failed to parse as an IP: %v", valueip)
			}
		}

		result, err := g.reader(g.handler, ip)
		if err != nil {
			g.mErr.Incr(1)
			g.log.Debugf("Failed to lookup geoip database: %v\n", err)
			return err
		}

		res := result.(GeoCoord)
		jsonObj := gabs.New()
		jsonObj.Set(res.geoip_lat, "geoip_lat")
		jsonObj.Set(res.geoip_lon, "geoip_lon")
		jsonObj.Set(res.geoip_country, "geoip_country")
		jsonObj.Set(res.geoip_city, "geoip_city")
		err = part.SetJSON(jsonObj)
		if err != nil {
			return err
		}
		return nil
	}

	IteratePartsWithSpanV2("GeoIP", g.parts, newMsg, proc)

	g.mBatchSent.Incr(1)
	g.mSent.Incr(int64(newMsg.Len()))
	return []types.Message{newMsg}, nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (g *GeoIP) CloseAsync() {
	g.handler.Close()
}

// WaitForClose blocks until the processor has closed down.
func (g *GeoIP) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
