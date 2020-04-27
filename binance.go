package binance

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/fatih/color"
	"github.com/gorilla/websocket"
	"github.com/json-iterator/go"
	"gitlab.roobee.develop/back/lib/common/common.git"
	"gitlab.roobee.develop/back/lib/common/errors.git"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const binanceWsUrl = "wss://stream.binance.com:9443/"
const binanceRestUrl = "https://api.binance.com/"

var timeDelta int64
var logger = color.New(color.FgGreen)
var client = &http.Client{}

type API struct {
	tradeKey    string
	tradeSecret string
	demo        bool
	client      *http.Client
	Log         common.Log
}

type BinanceTx struct {
	BinanceOrderId uint64
	ClientOrderId  string
	TxTime         uint64
	Type           string
	Amount         float64
	AvgPrice       float64
	BnbFee         float64
}

type Prices map[string]map[string]*Price // tokenTo : tokenFrom : Price
type Price struct {
	Ask      float64
	Bid      float64
	Close    float64
	TypeFrom string
	Updated  time.Time
}

type Order struct {
	Symbol              string `json:"symbol"`
	OrderID             int64  `json:"orderId"`
	OrderListID         int64  `json:"orderListId"`
	ClientOrderID       string `json:"clientOrderId"`
	Price               string `json:"price"`
	OrigQty             string `json:"origQty"`
	ExecutedQty         string `json:"executedQty"`
	CummulativeQuoteQty string `json:"cummulativeQuoteQty"`
	Status              string `json:"status"`
	TimeInForce         string `json:"timeInForce"`
	Type                string `json:"type"`
	Side                string `json:"side"`
	StopPrice           string `json:"stopPrice"`
	IcebergQty          string `json:"icebergQty"`
	Time                int64  `json:"time"`
	UpdateTime          int64  `json:"updateTime"`
	IsWorking           bool   `json:"isWorking"`
}

type ExchangeInfo struct {
	MinQty      float64 `json:"minQty"`
	MinNotional float64 `json:"minNotional"`
}

func init() {
	resp, err := request("GET", "api/v1/time", nil, nil)
	if err != nil {
		log.Fatal(err)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatal(err)
	}
	resp.Body.Close()

	type respFormat struct {
		ServerTime float64 `json:"serverTime"`
	}
	var jdata respFormat

	jsn := jsoniter.ConfigCompatibleWithStandardLibrary
	err = jsn.Unmarshal(body, &jdata)
	if err != nil {
		log.Fatal(err)
	}

	binanceTime := int64(jdata.ServerTime)
	timeDelta = binanceTime - time.Now().UTC().UnixNano()/1000000
	logger.Printf("- %-10s   connected [delta time: "+strconv.FormatInt(timeDelta, 10)+" ms]\n", "Binance:")
}

func request(method, path string, body io.Reader, header http.Header) (*http.Response, error) {
	req, err := http.NewRequest(method, binanceRestUrl+path, body)
	if err != nil {
		return nil, err
	}
	req.Header = header

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func New(tradeKey, tradeSecret string, demo bool, log common.Log) *API {
	return &API{
		tradeKey:    tradeKey,
		tradeSecret: tradeSecret,
		demo:        demo,
		client:      &http.Client{},
		Log:         log,
	}
}

func (api *API) Request(method, path string, form url.Values) (*http.Response, error) {
	var (
		URI  = binanceRestUrl + path
		body io.Reader
	)

	dataToSign := form.Encode()
	sign := createSign(api.tradeSecret, dataToSign)

	if method == "GET" {
		URI += `?` + dataToSign + "&signature=" + sign
	} else if method == "POST" {
		body = bytes.NewBuffer([]byte(dataToSign + "&signature=" + sign))
	}

	req, err := http.NewRequest(method, URI, body)
	if err != nil {
		return nil, err
	}
	req.Header.Add("X-MBX-APIKEY", api.tradeKey)

	resp, err := api.client.Do(req)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

// получает вендор баланс, если там есть что-то
func (api *API) BalanceGetList() (map[string]float64, error) {
	var (
		err  error
		resp *http.Response
		body []byte
	)

	var respFormat struct {
		Balances []struct {
			Token string `json:"asset"`
			Free  string `json:"free"`
		} `json:"balances"`
	}

	params := url.Values{}
	timestamp := time.Now().UTC().UnixNano()/1000000 + timeDelta

	params.Add("timestamp", strconv.FormatInt(timestamp, 10))

	if resp, err = api.Request("GET", "api/v3/account", params); err != nil {
		log.Println(err)
		return nil, err
	}

	defer resp.Body.Close()

	if body, err = ioutil.ReadAll(resp.Body); err != nil {
		log.Println(err)
	}

	if err = jsoniter.Unmarshal(body, &respFormat); err != nil {
		return nil, err
	}
	result := make(map[string]float64)
	for _, bal := range respFormat.Balances {
		amount, _ := strconv.ParseFloat(bal.Free, 64)
		if amount > 0 {
			result[bal.Token] = amount
		}
	}

	return result, nil
}

func (api *API) UpdatePrices(prices Prices, freq uint32, done chan bool) error {
	var (
		tokenFrom, tokenTo string
		pair               string
		err                error
		binanceConn        *websocket.Conn
		lastPingTime       time.Time
	)

	// Подключение к WS API Binance
	var binanceConnect = func() {
		binanceConn, _, err = websocket.DefaultDialer.Dial(binanceWsUrl+"ws/!miniTicker@arr", nil)
		if err != nil {
			api.Log("Binance API Connect error: "+err.Error(), common.ERROR)
			binanceConn = nil
			return
		}
		api.Log("Binance WS connected", common.INFO)
		lastPingTime = time.Now().UTC()

		binanceConn.SetPingHandler(func(message string) error {
			binanceConn.WriteControl(websocket.PongMessage, []byte(message), time.Time{})
			lastPingTime = time.Now().UTC()
			return nil
		})

		// горутина для обработки пакетов с котировками
		go func() {
			type miniTicker struct {
				Symbol     string `json:"s"`
				ClosePrice string `json:"c"`
			}
			var (
				jsn    = jsoniter.ConfigCompatibleWithStandardLibrary
				jstr   []miniTicker
				rawMsg []byte
			)

			for {
				if binanceConn == nil {
					break
				}

				if _, rawMsg, err = binanceConn.ReadMessage(); err != nil {
					api.Log("WS read error: "+err.Error(), common.ERROR)
					break
				}
				if err = jsn.Unmarshal(rawMsg, &jstr); err != nil {
					api.Log(err, common.ERROR)
					continue
				}
				for _, symbol := range jstr {
					pair = symbol.Symbol
					if pair == "BTCUSDT" {
						prices["BTC"]["USDT"].Close, err = strconv.ParseFloat(symbol.ClosePrice, 64)
						prices["BTC"]["USDT"].Updated = time.Now().UTC()
						prices["BTC"]["USDT"].TypeFrom = "crypto"
						continue
					} else if pair == "ETHUSDT" {
						prices["ETH"]["USDT"].Close, err = strconv.ParseFloat(symbol.ClosePrice, 64)
						prices["ETH"]["USDT"].Updated = time.Now().UTC()
						prices["ETH"]["USDT"].TypeFrom = "crypto"
						continue
					}

					for tokenTo, _ = range prices {
						if pair[len(pair)-len(tokenTo):] == tokenTo {
							tokenFrom = pair[:len(pair)-len(tokenTo)]
							if _, ok := prices[tokenTo][tokenFrom]; ok {
								prices[tokenTo][tokenFrom].Close, err = strconv.ParseFloat(symbol.ClosePrice, 64)
								prices[tokenTo][tokenFrom].Updated = time.Now().UTC()
								prices[tokenTo][tokenFrom].TypeFrom = "crypto"
							}
							break
						}
					}
				}
				select {
				case <-done:
					break
				default:
				}
			}

			// Если выбросило из цикла
			// Отправляем фрэйм закрытия соединения
			err = binanceConn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
			if err != nil {
				api.Log("WS write close error: "+err.Error(), common.ERROR)
				return
			}
			select {
			case <-time.After(time.Second * 2):
			}
			binanceConn.Close()
			binanceConn = nil
		}()

	}
	binanceConnect()

	// проверяем подключение к WS Binance
	// если нет подключения - пытаемся подключить
	dbConnTicker := time.NewTicker(30 * time.Second)
	go func() {
		for range dbConnTicker.C {

			if binanceConn == nil {
				binanceConnect()
			}

			if binanceConn != nil && time.Since(lastPingTime) > 4*time.Minute {
				api.Log("Binance WS connection lost", common.INFO)
				api.Log("Reconnect after 30s...", common.INFO)
				err = binanceConn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
				if err != nil {
					api.Log("WS write close error: "+err.Error(), common.ERROR)
					return
				}
				select {
				case <-time.After(time.Second * 1):
				}
				binanceConn.Close()
				binanceConn = nil
			}
			select {
			case <-done:
				break
			default:
			}
		}
	}()

	return nil
}

func createSign(key string, data string) string {
	h := hmac.New(sha256.New, []byte(key))
	h.Write([]byte(data))
	return hex.EncodeToString(h.Sum(nil))
}

func (api *API) OrderNew(tokenFrom, tokenTo string, amount float64) (*BinanceTx, error) {
	var (
		symbol    string
		err       error
		params    = url.Values{}
		resp      *http.Response
		body      []byte
		timestamp int64
		jsn       = jsoniter.ConfigCompatibleWithStandardLibrary
		avgPrice  float64
		totalCost float64
		fee       float64
		feeTotal  float64

		orderType = "market"
		side      string
	)

	if tokenFrom == "" {
		return nil, errors.New("tokenFrom:invalid")
	}
	if tokenTo == "" {
		return nil, errors.New("tokenTo:invalid")
	}
	if amount <= 0 {
		return nil, errors.New("amount:invalid")
	}

	if (tokenFrom == "BTC" || tokenFrom == "ETH") && tokenTo == "USDT" {
		symbol = tokenFrom + tokenTo
		side = "sell"
	} else if tokenFrom == "USDT" && (tokenTo == "BTC" || tokenTo == "ETH") {
		symbol = tokenTo + tokenFrom
		side = "buy"
	} else if tokenTo == "BTC" || tokenTo == "ETH" {
		symbol = tokenFrom + tokenTo
		side = "sell"
	} else if tokenFrom == "BTC" || tokenFrom == "ETH" {
		symbol = tokenTo + tokenFrom
		side = "buy"
	}

	timestamp = time.Now().UTC().UnixNano()/1000000 + timeDelta

	params.Add("symbol", symbol)
	params.Add("side", strings.ToUpper(side))
	params.Add("type", strings.ToUpper(orderType))
	params.Add("quantity", strconv.FormatFloat(amount, 'f', -1, 32))
	params.Add("timestamp", strconv.FormatInt(timestamp, 10))
	params.Add("recvWindow", strconv.Itoa(5000))

	if resp, err = api.Request("POST", "api/v3/order", params); err != nil {
		api.Log(err, common.ERROR)
		return nil, err
	}

	if body, err = ioutil.ReadAll(resp.Body); err != nil {
		api.Log(err, common.ERROR)
	}
	resp.Body.Close()

	type fillFormat struct {
		Commission      string `json:"commission"`
		CommissionAsset string `json:"commissionAsset"`
	}

	type respFormat struct {
		OrderId        uint64        `json:"orderId"`
		ClientOrderId  string        `json:"clientOrderId"`
		TxTime         uint64        `json:"transactTime"`
		CummulQuoteQty string        `json:"cummulativeQuoteQty"`
		Status         string        `json:"status"`
		Code           int           `json:"code"`
		Msg            string        `json:"msg"`
		Fills          []*fillFormat `json:"fills"`
	}

	jdata := respFormat{}
	var fill *fillFormat

	if err = jsn.Unmarshal(body, &jdata); err != nil {
		api.Log(err, common.ERROR)
		return nil, err
	}

	if jdata.Code != 0 {
		api.Log("Binance error: "+strconv.Itoa(jdata.Code)+" "+jdata.Msg, common.ERROR)
		return nil, errors.New(strconv.Itoa(jdata.Code) + " " + jdata.Msg)
	}

	if jdata.Status != "FILLED" {
		api.Log(string(body), common.ERROR)
		return nil, errors.New("Order id " + strconv.FormatUint(jdata.OrderId, 64) + " status: " + jdata.Status)
	}

	api.Log(string(body), common.INFO)

	if totalCost, err = strconv.ParseFloat(jdata.CummulQuoteQty, 64); err != nil {
		api.Log(err, common.ERROR)
	}
	avgPrice = totalCost / amount

	for _, fill = range jdata.Fills {
		if fee, err = strconv.ParseFloat(fill.Commission, 64); err != nil {
			api.Log(err, common.ERROR)
			return nil, err
		}
		feeTotal += fee
	}

	return &BinanceTx{
		BinanceOrderId: uint64(jdata.OrderId),
		ClientOrderId:  jdata.ClientOrderId,
		TxTime:         uint64(jdata.TxTime),
		Amount:         totalCost,
		Type:           side,
		AvgPrice:       avgPrice,
		BnbFee:         feeTotal,
	}, nil
}

func (api *API) OrderGetList(tokenFrom, tokenTo string) ([]Order, error) {
	var (
		respFormat []Order
		err        error
		params     = url.Values{}
		symbol     string
		timestamp  int64
		resp       *http.Response
		body       []byte
	)

	if (tokenFrom == "BTC" || tokenFrom == "ETH") && tokenTo == "USDT" {
		symbol = tokenFrom + tokenTo
	} else if tokenFrom == "USDT" && (tokenTo == "BTC" || tokenTo == "ETH") {
		symbol = tokenTo + tokenFrom
	} else if tokenTo == "BTC" || tokenTo == "ETH" {
		symbol = tokenFrom + tokenTo
	} else if tokenFrom == "BTC" || tokenFrom == "ETH" {
		symbol = tokenTo + tokenFrom
	}

	timestamp = time.Now().UTC().UnixNano()/1000000 + timeDelta
	params.Add("symbol", symbol)
	params.Add("timestamp", strconv.FormatInt(timestamp, 10))

	if resp, err = api.Request("GET", "api/v3/allOrders", params); err != nil {
		api.Log(err, common.ERROR)
		return nil, err
	}

	if body, err = ioutil.ReadAll(resp.Body); err != nil {
		api.Log(err, common.ERROR)
		log.Println(err)
	}
	defer resp.Body.Close()

	if err = jsoniter.Unmarshal(body, &respFormat); err != nil {
		api.Log(err, common.ERROR)
		log.Println(string(body))
		return nil, err
	}

	return respFormat, nil
}

// получить ордер
func (api *API) OrderGet(symbol, clientOrderId string) (*Order, error) {
	var (
		respFormat Order
		err        error
		params     = url.Values{}
		timestamp  int64
		resp       *http.Response
		body       []byte
	)

	timestamp = time.Now().UTC().UnixNano()/1000000 + timeDelta
	fmt.Println(symbol)
	params.Add("symbol", symbol)
	params.Add("origClientOrderId", clientOrderId)
	params.Add("timestamp", strconv.FormatInt(timestamp, 10))

	if resp, err = api.Request("GET", "api/v3/order", params); err != nil {
		api.Log(err, common.ERROR)
		return nil, err
	}

	if body, err = ioutil.ReadAll(resp.Body); err != nil {
		api.Log(err, common.ERROR)
		log.Println(err)
	}
	defer resp.Body.Close()

	if err = jsoniter.Unmarshal(body, &respFormat); err != nil {
		api.Log(err, common.ERROR)
		log.Println(string(body))
		return nil, err
	}

	return &respFormat, nil
}

func (api *API) LastTokenPricesSet(prices Prices) error {
	var (
		jsn      = jsoniter.ConfigCompatibleWithStandardLibrary
		resp     *http.Response
		respBody []byte

		err error
	)

	if resp, err = api.Request("GET", "api/v3/ticker/price", nil); err != nil {
		api.Log("LastPriceSet / Request "+err.Error(), common.ERROR)
		return err
	}
	defer resp.Body.Close()
	if respBody, err = ioutil.ReadAll(resp.Body); err != nil {
		api.Log("LastPriceSet / ReadAll "+err.Error(), common.ERROR)
		return err
	}
	var respStruct []struct {
		Symbol string `json:"symbol"`
		Price  string `json:"price"`
	}
	if err = jsn.Unmarshal(respBody, &respStruct); err != nil {
		api.Log("LastPriceSet / Unmarshal "+err.Error(), common.ERROR)
		return err
	}
	for _, price := range respStruct {
		pair := price.Symbol
		for tokenTo := range prices {
			if pair[len(pair)-len(tokenTo):] == tokenTo {
				tokenFrom := pair[:len(pair)-len(tokenTo)]
				if _, ok := prices[tokenTo][tokenFrom]; ok {
					prices[tokenTo][tokenFrom].Close, err = strconv.ParseFloat(price.Price, 64)
					prices[tokenTo][tokenFrom].Updated = time.Now().UTC()
					prices[tokenTo][tokenFrom].TypeFrom = "crypto"
				}
				break
			}
		}
	}

	api.Log("Successfully update lastTokenPrices", common.INFO)
	return nil
}

func ExchangeInfoGet() (map[string]map[string]ExchangeInfo, error) {
	type (
		exchangeResponse struct {
			Timezone   string `json:"timezone"`
			ServerTime int64  `json:"serverTime"`
			RateLimits []struct {
				RateLimitType string `json:"rateLimitType"`
				Interval      string `json:"interval"`
				IntervalNum   int    `json:"intervalNum"`
				Limit         int    `json:"limit"`
			} `json:"rateLimits"`
			ExchangeFilters []interface{} `json:"exchangeFilters"`
			Symbols         []struct {
				Symbol                 string   `json:"symbol"`
				Status                 string   `json:"status"`
				BaseAsset              string   `json:"baseAsset"`
				BaseAssetPrecision     int      `json:"baseAssetPrecision"`
				QuoteAsset             string   `json:"quoteAsset"`
				QuotePrecision         int      `json:"quotePrecision"`
				OrderTypes             []string `json:"orderTypes"`
				IcebergAllowed         bool     `json:"icebergAllowed"`
				IsSpotTradingAllowed   bool     `json:"isSpotTradingAllowed"`
				IsMarginTradingAllowed bool     `json:"isMarginTradingAllowed"`
				Filters                []struct {
					FilterType       string `json:"filterType"`
					MinPrice         string `json:"minPrice,omitempty"`
					MaxPrice         string `json:"maxPrice,omitempty"`
					TickSize         string `json:"tickSize,omitempty"`
					MultiplierUp     string `json:"multiplierUp,omitempty"`
					MultiplierDown   string `json:"multiplierDown,omitempty"`
					AvgPriceMins     int    `json:"avgPriceMins,omitempty"`
					MinQty           string `json:"minQty,omitempty"`
					MaxQty           string `json:"maxQty,omitempty"`
					StepSize         string `json:"stepSize,omitempty"`
					MinNotional      string `json:"minNotional,omitempty"`
					ApplyToMarket    bool   `json:"applyToMarket,omitempty"`
					Limit            int    `json:"limit,omitempty"`
					MaxNumAlgoOrders int    `json:"maxNumAlgoOrders,omitempty"`
				} `json:"filters"`
			} `json:"symbols"`
		}
	)
	var (
		jsn      = jsoniter.ConfigCompatibleWithStandardLibrary
		uri      = "https://api.binance.com/api/v1/exchangeInfo"
		resp     *http.Response
		err      error
		res      = make(map[string]map[string]ExchangeInfo)
		jsonBody []byte
		exchr    exchangeResponse
	)

	if resp, err = http.Get(uri); err != nil {
		return nil, err
	}

	if jsonBody, err = ioutil.ReadAll(resp.Body); err != nil {
		return nil, err
	}

	if err = jsn.Unmarshal(jsonBody, &exchr); err != nil {
		return nil, err
	}

	for _, symb := range exchr.Symbols {
		var (
			MinQty      float64
			MinNotional float64
		)

		if symb.QuoteAsset != "BTC" && symb.BaseAsset != "BTC" {
			continue
		}

		for _, filter := range symb.Filters {
			switch filter.FilterType {
			case "LOT_SIZE":
				MinQty, _ = strconv.ParseFloat(filter.MinQty, 64)
				break
			case "MIN_NOTIONAL":
				MinNotional, _ = strconv.ParseFloat(filter.MinNotional, 64)
				break
			}
		}
		if _, ok := res[symb.BaseAsset]; !ok {
			res[symb.BaseAsset] = make(map[string]ExchangeInfo)
		} else {
			continue
		}
		res[symb.BaseAsset][symb.QuoteAsset] = ExchangeInfo{
			MinQty:      MinQty,
			MinNotional: MinNotional,
		}
	}

	return res, nil
}

func KlineGetList(symbol, interval string, startTime time.Time) ([][]interface{}, error) {
	var (
		err  error
		resp *http.Response
		body []byte
		res  [][]interface{}
	)
	url := binanceRestUrl + "api/v1/klines?limit=1000&interval=" + interval + "&symbol=" + symbol
	if !startTime.IsZero() {
		url += "&startTime=" + strconv.FormatInt(startTime.Unix(), 10) + "001"
	}

	if resp, err = client.Get(url); err != nil {
		return nil, errors.Wrap(err)
	}
	if body, err = ioutil.ReadAll(resp.Body); err != nil {
		return nil, errors.Wrap(err)
	}

	if err = json.Unmarshal(body, &res); err != nil {
		return nil, errors.Wrap(err, url, string(body))
	}

	return res, nil
}
