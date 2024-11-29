package main

import (
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

type ClusterStats struct {
	ClusterName string `json:"cluster_name"`
	Status      string `json:"status"`
	Indices     struct {
		Count  int `json:"count"`
		Shards struct {
			Total int `json:"total"`
		} `json:"shards"`
		Docs struct {
			Count int `json:"count"`
		} `json:"docs"`
		Store struct {
			SizeInBytes      int64 `json:"size_in_bytes"`
			TotalSizeInBytes int64 `json:"total_size_in_bytes"`
		} `json:"store"`
	} `json:"indices"`
	Nodes struct {
		Total      int `json:"total"`
		Successful int `json:"successful"`
		Failed     int `json:"failed"`
	} `json:"_nodes"`
}

type NodesInfo struct {
	Nodes map[string]struct {
		Name             string   `json:"name"`
		TransportAddress string   `json:"transport_address"`
		Version          string   `json:"version"`
		Roles            []string `json:"roles"`
		OS               struct {
			AvailableProcessors int    `json:"available_processors"`
			Name                string `json:"name"`
			Arch                string `json:"arch"`
			Version             string `json:"version"`
			PrettyName          string `json:"pretty_name"`
		} `json:"os"`
		Process struct {
			ID       int  `json:"id"`
			Mlockall bool `json:"mlockall"`
		} `json:"process"`
		Settings struct {
			Node struct {
				Attr struct {
					ML struct {
						MachineMem string `json:"machine_memory"`
					} `json:"ml"`
				} `json:"attr"`
			} `json:"node"`
		} `json:"settings"`
	} `json:"nodes"`
}

type IndexStats []struct {
	Index     string `json:"index"`
	Health    string `json:"health"`
	DocsCount string `json:"docs.count"`
	StoreSize string `json:"store.size"`
	PriShards string `json:"pri"`
	Replicas  string `json:"rep"`
}

type IndexActivity struct {
	LastDocsCount    int
	IsActive         bool
	InitialDocsCount int
	StartTime        time.Time
}

type IndexWriteStats struct {
	Indices map[string]struct {
		Total struct {
			Indexing struct {
				IndexTotal int64 `json:"index_total"`
			} `json:"indexing"`
		} `json:"total"`
	} `json:"indices"`
}

type ClusterHealth struct {
	ActiveShards                int     `json:"active_shards"`
	ActivePrimaryShards         int     `json:"active_primary_shards"`
	RelocatingShards            int     `json:"relocating_shards"`
	InitializingShards          int     `json:"initializing_shards"`
	UnassignedShards            int     `json:"unassigned_shards"`
	DelayedUnassignedShards     int     `json:"delayed_unassigned_shards"`
	NumberOfPendingTasks        int     `json:"number_of_pending_tasks"`
	TaskMaxWaitingTime          string  `json:"task_max_waiting_time"`
	ActiveShardsPercentAsNumber float64 `json:"active_shards_percent_as_number"`
}

type NodesStats struct {
	Nodes map[string]struct {
		Indices struct {
			Store struct {
				SizeInBytes int64 `json:"size_in_bytes"`
			} `json:"store"`
			Search struct {
				QueryTotal        int64 `json:"query_total"`
				QueryTimeInMillis int64 `json:"query_time_in_millis"`
			} `json:"search"`
			Indexing struct {
				IndexTotal        int64 `json:"index_total"`
				IndexTimeInMillis int64 `json:"index_time_in_millis"`
			} `json:"indexing"`
			Segments struct {
				Count int64 `json:"count"`
			} `json:"segments"`
		} `json:"indices"`
		OS struct {
			CPU struct {
				Percent int `json:"percent"`
			} `json:"cpu"`
			Memory struct {
				UsedInBytes  int64 `json:"used_in_bytes"`
				FreeInBytes  int64 `json:"free_in_bytes"`
				TotalInBytes int64 `json:"total_in_bytes"`
			} `json:"mem"`
			LoadAverage map[string]float64 `json:"load_average"`
		} `json:"os"`
		JVM struct {
			Memory struct {
				HeapUsedInBytes int64 `json:"heap_used_in_bytes"`
				HeapMaxInBytes  int64 `json:"heap_max_in_bytes"`
			} `json:"mem"`
			GC struct {
				Collectors struct {
					Young struct {
						CollectionCount        int64 `json:"collection_count"`
						CollectionTimeInMillis int64 `json:"collection_time_in_millis"`
					} `json:"young"`
					Old struct {
						CollectionCount        int64 `json:"collection_count"`
						CollectionTimeInMillis int64 `json:"collection_time_in_millis"`
					} `json:"old"`
				} `json:"collectors"`
			} `json:"gc"`
		} `json:"jvm"`
		Transport struct {
			RxSizeInBytes int64 `json:"rx_size_in_bytes"`
			TxSizeInBytes int64 `json:"tx_size_in_bytes"`
			RxCount       int64 `json:"rx_count"`
			TxCount       int64 `json:"tx_count"`
		} `json:"transport"`
		HTTP struct {
			CurrentOpen int64 `json:"current_open"`
		} `json:"http"`
		Process struct {
			OpenFileDescriptors int64 `json:"open_file_descriptors"`
		} `json:"process"`
		FS struct {
			DiskReads  int64 `json:"disk_reads"`
			DiskWrites int64 `json:"disk_writes"`
			Total      struct {
				TotalInBytes     int64 `json:"total_in_bytes"`
				FreeInBytes      int64 `json:"free_in_bytes"`
				AvailableInBytes int64 `json:"available_in_bytes"`
			} `json:"total"`
			Data []struct {
				Path             string `json:"path"`
				TotalInBytes     int64  `json:"total_in_bytes"`
				FreeInBytes      int64  `json:"free_in_bytes"`
				AvailableInBytes int64  `json:"available_in_bytes"`
			} `json:"data"`
		} `json:"fs"`
	} `json:"nodes"`
}

type GitHubRelease struct {
	TagName string `json:"tag_name"`
}

var (
	latestVersion string
	versionCache  time.Time
)

var indexActivities = make(map[string]*IndexActivity)

type IngestionEvent struct {
	Index     string
	DocCount  int
	Timestamp time.Time
}

type CatNodesStats struct {
	Load1m string `json:"load_1m"`
	Name   string `json:"name"`
}

var (
	showNodes         = true
	showRoles         = true
	showIndices       = true
	showMetrics       = true
	showHiddenIndices = false
)

var (
	header       *tview.TextView
	nodesPanel   *tview.TextView
	rolesPanel   *tview.TextView
	indicesPanel *tview.TextView
	metricsPanel *tview.TextView
)

func bytesToHuman(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}

	units := []string{"B", "K", "M", "G", "T", "P", "E", "Z"}
	exp := 0
	val := float64(bytes)

	for val >= unit && exp < len(units)-1 {
		val /= unit
		exp++
	}

	return fmt.Sprintf("%.1f%s", val, units[exp])
}

func formatNumber(n int) string {
	str := fmt.Sprintf("%d", n)

	var result []rune
	for i, r := range str {
		if i > 0 && (len(str)-i)%3 == 0 {
			result = append(result, ',')
		}
		result = append(result, r)
	}
	return string(result)
}

func convertSizeFormat(sizeStr string) string {
	var size float64
	var unit string
	fmt.Sscanf(sizeStr, "%f%s", &size, &unit)

	unit = strings.ToUpper(strings.TrimSuffix(unit, "b"))

	return fmt.Sprintf("%d%s", int(size), unit)
}

func formatResourceSize(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%4d B", bytes)
	}

	units := []string{"B", "K", "M", "G", "T", "P"}
	exp := 0
	val := float64(bytes)

	for val >= unit && exp < len(units)-1 {
		val /= unit
		exp++
	}

	return fmt.Sprintf("%3d%s", int(val), units[exp])
}

func getPercentageColor(percent float64) string {
	switch {
	case percent < 30:
		return "green"
	case percent < 70:
		return "#00ffff" // cyan
	case percent < 85:
		return "#ffff00" // yellow
	default:
		return "#ff5555" // light red
	}
}

func getLatestVersion() string {
	// Only fetch every hour
	if time.Since(versionCache) < time.Hour && latestVersion != "" {
		return latestVersion
	}

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get("https://api.github.com/repos/elastic/elasticsearch/releases/latest")
	if err != nil {
		return ""
	}
	defer resp.Body.Close()

	var release GitHubRelease
	if err := json.NewDecoder(resp.Body).Decode(&release); err != nil {
		return ""
	}

	latestVersion = strings.TrimPrefix(release.TagName, "v")
	versionCache = time.Now()
	return latestVersion
}

func compareVersions(current, latest string) bool {
	if latest == "" {
		return true
	}

	// Clean up version strings
	current = strings.TrimPrefix(current, "v")
	latest = strings.TrimPrefix(latest, "v")

	// Split versions into parts
	currentParts := strings.Split(current, ".")
	latestParts := strings.Split(latest, ".")

	// Compare each part
	for i := 0; i < len(currentParts) && i < len(latestParts); i++ {
		curr, _ := strconv.Atoi(currentParts[i])
		lat, _ := strconv.Atoi(latestParts[i])
		if curr != lat {
			return curr >= lat
		}
	}
	return len(currentParts) >= len(latestParts)
}

var roleColors = map[string]string{
	"master":                "#ff5555", // red
	"data":                  "#50fa7b", // green
	"data_content":          "#8be9fd", // cyan
	"data_hot":              "#ffb86c", // orange
	"data_warm":             "#bd93f9", // purple
	"data_cold":             "#f1fa8c", // yellow
	"data_frozen":           "#ff79c6", // pink
	"ingest":                "#87cefa", // light sky blue (was gray)
	"ml":                    "#6272a4", // blue gray
	"remote_cluster_client": "#dda0dd", // plum (was burgundy)
	"transform":             "#689d6a", // forest green
	"voting_only":           "#458588", // teal
	"coordinating_only":     "#d65d0e", // burnt orange
}

var legendLabels = map[string]string{
	"master":                "Master",
	"data":                  "Data",
	"data_content":          "Data Content",
	"data_hot":              "Data Hot",
	"data_warm":             "Data Warm",
	"data_cold":             "Data Cold",
	"data_frozen":           "Data Frozen",
	"ingest":                "Ingest",
	"ml":                    "Machine Learning",
	"remote_cluster_client": "Remote Cluster Client",
	"transform":             "Transform",
	"voting_only":           "Voting Only",
	"coordinating_only":     "Coordinating Only",
}

func formatNodeRoles(roles []string) string {
	roleMap := map[string]string{
		"master":                "M",
		"data":                  "D",
		"data_content":          "C",
		"data_hot":              "H",
		"data_warm":             "W",
		"data_cold":             "K",
		"data_frozen":           "F",
		"ingest":                "I",
		"ml":                    "L",
		"remote_cluster_client": "R",
		"transform":             "T",
		"voting_only":           "V",
		"coordinating_only":     "O",
	}

	// Get the role letters and sort them
	var letters []string
	for _, role := range roles {
		if letter, exists := roleMap[role]; exists {
			letters = append(letters, letter)
		}
	}
	sort.Strings(letters)

	formattedRoles := "             "
	runeRoles := []rune(formattedRoles)

	for i, letter := range letters {
		if i < 13 {
			runeRoles[i] = []rune(letter)[0]
		}
	}

	var result string
	for _, r := range runeRoles {
		if r == ' ' {
			result += " "
		} else {
			for role, shortRole := range roleMap {
				if string(r) == shortRole {
					result += fmt.Sprintf("[%s]%s[white]", roleColors[role], string(r))
					break
				}
			}
		}
	}

	return result
}

func getHealthColor(health string) string {
	switch health {
	case "green":
		return "green"
	case "yellow":
		return "#ffff00" // yellow
	case "red":
		return "#ff5555" // light red
	default:
		return "white"
	}
}

type indexInfo struct {
	index        string
	health       string
	docs         int
	storeSize    string
	priShards    string
	replicas     string
	writeOps     int64
	indexingRate float64
}

// Add startTime at package level
var startTime = time.Now()

func updateGridLayout(grid *tview.Grid, showRoles, showIndices, showMetrics bool) {
	// Start with clean grid
	grid.Clear()

	visiblePanels := make([]struct {
		panel *tview.TextView
		show  bool
	}, 0)

	if showRoles {
		visiblePanels = append(visiblePanels, struct {
			panel *tview.TextView
			show  bool
		}{rolesPanel, true})
	}
	if showIndices {
		visiblePanels = append(visiblePanels, struct {
			panel *tview.TextView
			show  bool
		}{indicesPanel, true})
	}
	if showMetrics {
		visiblePanels = append(visiblePanels, struct {
			panel *tview.TextView
			show  bool
		}{metricsPanel, true})
	}

	// Adjust row configuration based on whether nodes panel is shown
	if showNodes {
		grid.SetRows(3, 0, 0) // Header, nodes, bottom panels
	} else {
		grid.SetRows(3, 0) // Just header and bottom panels
	}

	// Set up columns based on number of visible panels
	switch len(visiblePanels) {
	case 3:
		grid.SetColumns(-1, -2, -1) // 1:2:1 ratio
	case 2:
		grid.SetColumns(-1, -1) // Two equal columns
	case 1:
		grid.SetColumns(-1) // Single column
	}

	// Always show header at top spanning all columns
	grid.AddItem(header, 0, 0, 1, len(visiblePanels), 0, 0, false)

	// Add nodes panel if visible, spanning all columns
	if showNodes {
		grid.AddItem(nodesPanel, 1, 0, 1, len(visiblePanels), 0, 0, false)

		// Add bottom panels
		for i, panel := range visiblePanels {
			grid.AddItem(panel.panel, 2, i, 1, 1, 0, 0, false)
		}
	} else {
		// Add bottom panels starting from row 1
		for i, panel := range visiblePanels {
			grid.AddItem(panel.panel, 1, i, 1, 1, 0, 0, false)
		}
	}
}

func main() {
	host := flag.String("host", "http://localhost", "Elasticsearch host URL (e.g., http://localhost or https://example.com)")
	port := flag.Int("port", 9200, "Elasticsearch port")
	user := flag.String("user", "elastic", "Elasticsearch username")
	password := flag.String("password", os.Getenv("ES_PASSWORD"), "Elasticsearch password")
	flag.Parse()

	// Validate and process the host URL
	if !strings.HasPrefix(*host, "http://") && !strings.HasPrefix(*host, "https://") {
		fmt.Fprintf(os.Stderr, "Error: host must start with http:// or https://\n")
		os.Exit(1)
	}

	// Strip any trailing slash from the host
	*host = strings.TrimRight(*host, "/")

	// Create custom HTTP client with SSL configuration
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true, // Allow self-signed certificates
		},
	}
	client := &http.Client{
		Transport: tr,
		Timeout:   time.Second * 10,
	}

	app := tview.NewApplication()

	// Update the grid layout to use three columns for the bottom section
	grid := tview.NewGrid().
		SetRows(3, 0, 0).       // Three rows: header, nodes, bottom panels
		SetColumns(-1, -2, -1). // Three columns for bottom row: roles (1), indices (2), metrics (1)
		SetBorders(true)

	// Initialize the panels (move initialization to package level)
	header = tview.NewTextView().
		SetDynamicColors(true).
		SetTextAlign(tview.AlignLeft)

	nodesPanel = tview.NewTextView().
		SetDynamicColors(true)

	rolesPanel = tview.NewTextView(). // New panel for roles
						SetDynamicColors(true)

	indicesPanel = tview.NewTextView().
		SetDynamicColors(true)

	metricsPanel = tview.NewTextView().
		SetDynamicColors(true)

	// Initial layout
	updateGridLayout(grid, showRoles, showIndices, showMetrics)

	// Add panels to grid
	grid.AddItem(header, 0, 0, 1, 3, 0, 0, false). // Header spans all columns
							AddItem(nodesPanel, 1, 0, 1, 3, 0, 0, false).   // Nodes panel spans all columns
							AddItem(rolesPanel, 2, 0, 1, 1, 0, 0, false).   // Roles panel in left column
							AddItem(indicesPanel, 2, 1, 1, 1, 0, 0, false). // Indices panel in middle column
							AddItem(metricsPanel, 2, 2, 1, 1, 0, 0, false)  // Metrics panel in right column

	// Update function
	update := func() {
		baseURL := fmt.Sprintf("%s:%d", *host, *port)

		// Helper function for ES requests
		makeRequest := func(path string, target interface{}) error {
			req, err := http.NewRequest("GET", baseURL+path, nil)
			if err != nil {
				return err
			}
			req.SetBasicAuth(*user, *password)
			resp, err := client.Do(req)
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				return err
			}
			return json.Unmarshal(body, target)
		}

		// Get cluster stats
		var clusterStats ClusterStats
		if err := makeRequest("/_cluster/stats", &clusterStats); err != nil {
			header.SetText(fmt.Sprintf("[red]Error: %v", err))
			return
		}

		// Get nodes info
		var nodesInfo NodesInfo
		if err := makeRequest("/_nodes", &nodesInfo); err != nil {
			nodesPanel.SetText(fmt.Sprintf("[red]Error: %v", err))
			return
		}

		// Get indices stats
		var indicesStats IndexStats
		if err := makeRequest("/_cat/indices?format=json", &indicesStats); err != nil {
			indicesPanel.SetText(fmt.Sprintf("[red]Error: %v", err))
			return
		}

		// Get cluster health
		var clusterHealth ClusterHealth
		if err := makeRequest("/_cluster/health", &clusterHealth); err != nil {
			indicesPanel.SetText(fmt.Sprintf("[red]Error: %v", err))
			return
		}

		// Get nodes stats
		var nodesStats NodesStats
		if err := makeRequest("/_nodes/stats", &nodesStats); err != nil {
			indicesPanel.SetText(fmt.Sprintf("[red]Error: %v", err))
			return
		}

		// Get index write stats
		var indexWriteStats IndexWriteStats
		if err := makeRequest("/_stats", &indexWriteStats); err != nil {
			indicesPanel.SetText(fmt.Sprintf("[red]Error getting write stats: %v", err))
			return
		}

		// Calculate aggregate metrics
		var (
			totalQueries       int64
			totalQueryTime     float64
			totalIndexing      int64
			totalIndexingTime  float64
			totalCPUPercent    int
			totalMemoryUsed    int64
			totalMemoryTotal   int64
			totalHeapUsed      int64
			totalHeapMax       int64
			totalGCCollections int64
			totalGCTime        float64
			nodeCount          int
		)

		for _, node := range nodesStats.Nodes {
			totalQueries += node.Indices.Search.QueryTotal
			totalQueryTime += float64(node.Indices.Search.QueryTimeInMillis) / 1000
			totalIndexing += node.Indices.Indexing.IndexTotal
			totalIndexingTime += float64(node.Indices.Indexing.IndexTimeInMillis) / 1000
			totalCPUPercent += node.OS.CPU.Percent
			totalMemoryUsed += node.OS.Memory.UsedInBytes
			totalMemoryTotal += node.OS.Memory.TotalInBytes
			totalHeapUsed += node.JVM.Memory.HeapUsedInBytes
			totalHeapMax += node.JVM.Memory.HeapMaxInBytes
			totalGCCollections += node.JVM.GC.Collectors.Young.CollectionCount + node.JVM.GC.Collectors.Old.CollectionCount
			totalGCTime += float64(node.JVM.GC.Collectors.Young.CollectionTimeInMillis+node.JVM.GC.Collectors.Old.CollectionTimeInMillis) / 1000
			nodeCount++
		}

		// Update header
		statusColor := map[string]string{
			"green":  "green",
			"yellow": "yellow",
			"red":    "red",
		}[clusterStats.Status]

		// Get max lengths after fetching node and index info
		maxNodeNameLen, maxIndexNameLen := getMaxLengths(nodesInfo, indicesStats)

		// Update header with dynamic padding
		header.Clear()
		latestVer := getLatestVersion()
		padding := 0
		if maxNodeNameLen > len(clusterStats.ClusterName) {
			padding = maxNodeNameLen - len(clusterStats.ClusterName)
		}
		fmt.Fprintf(header, "[#00ffff]Cluster:[white] %s [%s]%s[-]%s[#00ffff]Latest: [white]%s\n",
			clusterStats.ClusterName,
			statusColor,
			strings.ToUpper(clusterStats.Status),
			strings.Repeat(" ", padding),
			latestVer)
		fmt.Fprintf(header, "[#00ffff]Nodes:[white] %d Total, [green]%d[white] Successful, [#ff5555]%d[white] Failed\n",
			clusterStats.Nodes.Total,
			clusterStats.Nodes.Successful,
			clusterStats.Nodes.Failed)
		fmt.Fprintf(header, "[#666666]Press 2-5 to toggle panels, 'h' to toggle hidden indices, 'q' to quit[white]\n")

		// Update nodes panel with dynamic width
		nodesPanel.Clear()
		fmt.Fprintf(nodesPanel, "[::b][#00ffff][[#ff5555]2[#00ffff]] Nodes Information[::-]\n\n")
		fmt.Fprint(nodesPanel, getNodesPanelHeader(maxNodeNameLen))

		// Update node entries with dynamic width
		for id, nodeInfo := range nodesInfo.Nodes {
			nodeStats, exists := nodesStats.Nodes[id]
			if !exists {
				continue
			}

			// Calculate resource percentages and format memory values
			cpuPercent := nodeStats.OS.CPU.Percent
			memPercent := float64(nodeStats.OS.Memory.UsedInBytes) / float64(nodeStats.OS.Memory.TotalInBytes) * 100
			heapPercent := float64(nodeStats.JVM.Memory.HeapUsedInBytes) / float64(nodeStats.JVM.Memory.HeapMaxInBytes) * 100

			// Calculate disk usage - use the data path stats
			diskTotal := int64(0)
			diskAvailable := int64(0)
			if len(nodeStats.FS.Data) > 0 {
				// Use the first data path's stats - this is the Elasticsearch data directory
				diskTotal = nodeStats.FS.Data[0].TotalInBytes
				diskAvailable = nodeStats.FS.Data[0].AvailableInBytes
			} else {
				// Fallback to total stats if data path stats aren't available
				diskTotal = nodeStats.FS.Total.TotalInBytes
				diskAvailable = nodeStats.FS.Total.AvailableInBytes
			}
			diskUsed := diskTotal - diskAvailable
			diskPercent := float64(diskUsed) / float64(diskTotal) * 100

			versionColor := "yellow"
			if compareVersions(nodeInfo.Version, latestVer) {
				versionColor = "green"
			}

			// Add this request before the nodes panel update
			var catNodesStats []CatNodesStats
			if err := makeRequest("/_cat/nodes?format=json&h=name,load_1m", &catNodesStats); err != nil {
				nodesPanel.SetText(fmt.Sprintf("[red]Error getting cat nodes stats: %v", err))
				return
			}

			// Create a map for quick lookup of load averages by node name
			nodeLoads := make(map[string]string)
			for _, node := range catNodesStats {
				nodeLoads[node.Name] = node.Load1m
			}

			fmt.Fprintf(nodesPanel, "[#5555ff]%-*s  [white] [#444444]│[white] %s [#444444]│[white] [white]%-20s[white] [#444444]│[white] [%s]%-7s[white] [#444444]│[white] [%s]%3d%% [#444444](%d)[white] [#444444]│[white] %4s [#444444]│[white] %4s / %4s [%s]%3d%%[white] [#444444]│[white] %4s / %4s [%s]%3d%%[white] [#444444]│[white] %4s / %4s [%s]%3d%%[white] [#444444]│[white] %s [#bd93f9]%s[white] [#444444](%s)[white]\n",
				maxNodeNameLen,
				nodeInfo.Name,
				formatNodeRoles(nodeInfo.Roles),
				nodeInfo.TransportAddress,
				versionColor,
				nodeInfo.Version,
				getPercentageColor(float64(cpuPercent)),
				cpuPercent,
				nodeInfo.OS.AvailableProcessors,
				nodeLoads[nodeInfo.Name],
				formatResourceSize(nodeStats.OS.Memory.UsedInBytes),
				formatResourceSize(nodeStats.OS.Memory.TotalInBytes),
				getPercentageColor(memPercent),
				int(memPercent),
				formatResourceSize(nodeStats.JVM.Memory.HeapUsedInBytes),
				formatResourceSize(nodeStats.JVM.Memory.HeapMaxInBytes),
				getPercentageColor(heapPercent),
				int(heapPercent),
				formatResourceSize(diskUsed),
				formatResourceSize(diskTotal),
				getPercentageColor(diskPercent),
				int(diskPercent),
				nodeInfo.OS.PrettyName,
				nodeInfo.OS.Version,
				nodeInfo.OS.Arch)
		}

		// Update indices panel with dynamic width
		indicesPanel.Clear()
		fmt.Fprintf(indicesPanel, "[::b][#00ffff][[#ff5555]4[#00ffff]] Indices Information[::-]\n\n")
		fmt.Fprint(indicesPanel, getIndicesPanelHeader(maxIndexNameLen))

		// Update index entries with dynamic width
		var indices []indexInfo
		var totalDocs int
		var totalSize int64

		// Collect index information
		for _, index := range indicesStats {
			// Skip hidden indices unless showHiddenIndices is true
			if (!showHiddenIndices && strings.HasPrefix(index.Index, ".")) || index.DocsCount == "0" {
				continue
			}
			docs := 0
			fmt.Sscanf(index.DocsCount, "%d", &docs)
			totalDocs += docs

			// Track document changes
			activity, exists := indexActivities[index.Index]
			if !exists {
				indexActivities[index.Index] = &IndexActivity{
					LastDocsCount:    docs,
					InitialDocsCount: docs,
					StartTime:        time.Now(),
				}
			} else {
				activity.LastDocsCount = docs
			}

			// Get write operations count and calculate rate
			writeOps := int64(0)
			indexingRate := float64(0)
			if stats, exists := indexWriteStats.Indices[index.Index]; exists {
				writeOps = stats.Total.Indexing.IndexTotal
				if activity, ok := indexActivities[index.Index]; ok {
					timeDiff := time.Since(activity.StartTime).Seconds()
					if timeDiff > 0 {
						indexingRate = float64(docs-activity.InitialDocsCount) / timeDiff
					}
				}
			}

			indices = append(indices, indexInfo{
				index:        index.Index,
				health:       index.Health,
				docs:         docs,
				storeSize:    index.StoreSize,
				priShards:    index.PriShards,
				replicas:     index.Replicas,
				writeOps:     writeOps,
				indexingRate: indexingRate,
			})
		}

		// Calculate total size
		for _, node := range nodesStats.Nodes {
			totalSize += node.FS.Total.TotalInBytes - node.FS.Total.AvailableInBytes
		}

		// Sort indices by document count (descending)
		sort.Slice(indices, func(i, j int) bool {
			return indices[i].docs > indices[j].docs
		})

		// Update index entries with dynamic width
		for _, idx := range indices {
			// Only show purple dot if there's actual indexing happening
			writeIcon := "[#444444]⚪"
			if idx.indexingRate > 0 {
				writeIcon = "[#5555ff]⚫"
			}

			// Calculate document changes
			activity := indexActivities[idx.index]
			ingestedStr := ""
			if activity != nil && activity.InitialDocsCount < idx.docs {
				docChange := idx.docs - activity.InitialDocsCount
				ingestedStr = fmt.Sprintf("[green]+%-11s", formatNumber(docChange))
			} else {
				ingestedStr = fmt.Sprintf("%-12s", "") // Empty space if no changes
			}

			// Format indexing rate
			rateStr := ""
			if idx.indexingRate > 0 {
				if idx.indexingRate >= 1000 {
					rateStr = fmt.Sprintf("[#50fa7b]%.1fk/s", idx.indexingRate/1000)
				} else {
					rateStr = fmt.Sprintf("[#50fa7b]%.1f/s", idx.indexingRate)
				}
			} else {
				rateStr = "[#444444]0/s"
			}

			// Convert the size format before display
			sizeStr := convertSizeFormat(idx.storeSize)

			fmt.Fprintf(indicesPanel, "%s [%s]%-*s  [white][#444444]│[white] %15s [#444444]│[white] %12s [#444444]│[white] %8s [#444444]│[white] %8s [#444444]│[white] %s [#444444]│[white] %-10s\n",
				writeIcon,
				getHealthColor(idx.health),
				maxIndexNameLen,
				idx.index,
				formatNumber(idx.docs),
				sizeStr,
				idx.priShards,
				idx.replicas,
				ingestedStr,
				rateStr)
		}

		// Calculate total indexing rate for the cluster
		totalIndexingRate := float64(0)
		for _, idx := range indices {
			totalIndexingRate += idx.indexingRate
		}

		// Format cluster indexing rate
		clusterRateStr := ""
		if totalIndexingRate > 0 {
			if totalIndexingRate >= 1000000 {
				clusterRateStr = fmt.Sprintf("[#50fa7b]%.1fM/s", totalIndexingRate/1000000)
			} else if totalIndexingRate >= 1000 {
				clusterRateStr = fmt.Sprintf("[#50fa7b]%.1fK/s", totalIndexingRate/1000)
			} else {
				clusterRateStr = fmt.Sprintf("[#50fa7b]%.1f/s", totalIndexingRate)
			}
		} else {
			clusterRateStr = "[#444444]0/s"
		}

		// Display the totals with indexing rate
		fmt.Fprintf(indicesPanel, "\n[#00ffff]Total Documents:[white] %s, [#00ffff]Total Size:[white] %s, [#00ffff]Indexing Rate:[white] %s\n",
			formatNumber(totalDocs),
			bytesToHuman(totalSize),
			clusterRateStr)

		// Move shard stats to bottom of indices panel
		fmt.Fprintf(indicesPanel, "\n[#00ffff]Shard Status:[white] Active: %d (%.1f%%), Primary: %d, Relocating: %d, Initializing: %d, Unassigned: %d\n",
			clusterHealth.ActiveShards,
			clusterHealth.ActiveShardsPercentAsNumber,
			clusterHealth.ActivePrimaryShards,
			clusterHealth.RelocatingShards,
			clusterHealth.InitializingShards,
			clusterHealth.UnassignedShards)

		// Update metrics panel
		metricsPanel.Clear()
		fmt.Fprintf(metricsPanel, "[::b][#00ffff][[#ff5555]5[#00ffff]] Cluster Metrics[::-]\n\n")

		// Helper function to format metric lines with consistent alignment
		formatMetric := func(name string, value string) string {
			return fmt.Sprintf("[#00ffff]%-25s[white] %s\n", name+":", value)
		}

		// Search metrics
		fmt.Fprint(metricsPanel, formatMetric("Search Queries", formatNumber(int(totalQueries))))
		fmt.Fprint(metricsPanel, formatMetric("Query Rate", fmt.Sprintf("%s/s", formatNumber(int(float64(totalQueries)/time.Since(startTime).Seconds())))))
		fmt.Fprint(metricsPanel, formatMetric("Total Query Time", fmt.Sprintf("%.1fs", totalQueryTime)))
		fmt.Fprint(metricsPanel, formatMetric("Avg Query Latency", fmt.Sprintf("%.2fms", totalQueryTime*1000/float64(totalQueries+1))))

		// Indexing metrics
		fmt.Fprint(metricsPanel, formatMetric("Index Operations", formatNumber(int(totalIndexing))))
		fmt.Fprint(metricsPanel, formatMetric("Indexing Rate", fmt.Sprintf("%s/s", formatNumber(int(float64(totalIndexing)/time.Since(startTime).Seconds())))))
		fmt.Fprint(metricsPanel, formatMetric("Total Index Time", fmt.Sprintf("%.1fs", totalIndexingTime)))
		fmt.Fprint(metricsPanel, formatMetric("Avg Index Latency", fmt.Sprintf("%.2fms", totalIndexingTime*1000/float64(totalIndexing+1))))

		// GC metrics
		fmt.Fprint(metricsPanel, formatMetric("GC Collections", formatNumber(int(totalGCCollections))))
		fmt.Fprint(metricsPanel, formatMetric("Total GC Time", fmt.Sprintf("%.1fs", totalGCTime)))
		fmt.Fprint(metricsPanel, formatMetric("Avg GC Time", fmt.Sprintf("%.2fms", totalGCTime*1000/float64(totalGCCollections+1))))

		// Memory metrics
		totalMemoryPercent := float64(totalMemoryUsed) / float64(totalMemoryTotal) * 100
		totalHeapPercent := float64(totalHeapUsed) / float64(totalHeapMax) * 100
		fmt.Fprint(metricsPanel, formatMetric("Memory Usage", fmt.Sprintf("%s / %s (%.1f%%)", bytesToHuman(totalMemoryUsed), bytesToHuman(totalMemoryTotal), totalMemoryPercent)))
		fmt.Fprint(metricsPanel, formatMetric("Heap Usage", fmt.Sprintf("%s / %s (%.1f%%)", bytesToHuman(totalHeapUsed), bytesToHuman(totalHeapMax), totalHeapPercent)))

		// Segment metrics
		fmt.Fprint(metricsPanel, formatMetric("Total Segments", formatNumber(int(getTotalSegments(nodesStats)))))
		fmt.Fprint(metricsPanel, formatMetric("Open File Descriptors", formatNumber(int(getTotalOpenFiles(nodesStats)))))

		// Network metrics
		fmt.Fprint(metricsPanel, formatMetric("Network TX", bytesToHuman(getTotalNetworkTX(nodesStats))))
		fmt.Fprint(metricsPanel, formatMetric("Network RX", bytesToHuman(getTotalNetworkRX(nodesStats))))

		// Disk I/O metrics
		totalDiskReads := int64(0)
		totalDiskWrites := int64(0)
		for _, node := range nodesStats.Nodes {
			totalDiskReads += node.FS.DiskReads
			totalDiskWrites += node.FS.DiskWrites
		}
		fmt.Fprint(metricsPanel, formatMetric("Disk Reads", formatNumber(int(totalDiskReads))))
		fmt.Fprint(metricsPanel, formatMetric("Disk Writes", formatNumber(int(totalDiskWrites))))

		// HTTP connections
		totalHTTPConnections := int64(0)
		for _, node := range nodesStats.Nodes {
			totalHTTPConnections += node.HTTP.CurrentOpen
		}
		fmt.Fprint(metricsPanel, formatMetric("HTTP Connections", formatNumber(int(totalHTTPConnections))))

		// Average CPU usage across nodes
		avgCPUPercent := float64(totalCPUPercent) / float64(nodeCount)
		fmt.Fprint(metricsPanel, formatMetric("Average CPU Usage", fmt.Sprintf("%.1f%%", avgCPUPercent)))

		// Pending tasks
		fmt.Fprint(metricsPanel, formatMetric("Pending Tasks", formatNumber(clusterHealth.NumberOfPendingTasks)))
		if clusterHealth.TaskMaxWaitingTime != "" && clusterHealth.TaskMaxWaitingTime != "0s" {
			fmt.Fprint(metricsPanel, formatMetric("Max Task Wait Time", clusterHealth.TaskMaxWaitingTime))
		}

		// Update roles panel
		rolesPanel.Clear()
		fmt.Fprintf(rolesPanel, "[::b][#00ffff][[#ff5555]3[#00ffff]] Node Roles[::-]\n\n")

		// Create a map of used roles
		usedRoles := make(map[string]bool)
		for _, nodeInfo := range nodesInfo.Nodes {
			for _, role := range nodeInfo.Roles {
				usedRoles[role] = true
			}
		}

		// Display roles in the roles panel
		roleLegend := [][2]string{
			{"C", "data_content"},
			{"D", "data"},
			{"F", "data_frozen"},
			{"H", "data_hot"},
			{"I", "ingest"},
			{"K", "data_cold"},
			{"L", "ml"},
			{"M", "master"},
			{"O", "coordinating_only"},
			{"R", "remote_cluster_client"},
			{"T", "transform"},
			{"V", "voting_only"},
			{"W", "data_warm"},
		}

		for _, role := range roleLegend {
			if usedRoles[role[1]] {
				fmt.Fprintf(rolesPanel, "[%s]%s[white] %s\n",
					roleColors[role[1]],
					role[0],
					legendLabels[role[1]])
			} else {
				fmt.Fprintf(rolesPanel, "[#444444]%s %s\n",
					role[0],
					legendLabels[role[1]])
			}
		}
	}

	// Set up periodic updates
	go func() {
		for {
			app.QueueUpdateDraw(func() {
				update()
			})
			time.Sleep(5 * time.Second)
		}
	}()

	// Handle quit
	app.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		switch event.Key() {
		case tcell.KeyEsc:
			app.Stop()
		case tcell.KeyRune:
			switch event.Rune() {
			case 'q':
				app.Stop()
			case '2':
				showNodes = !showNodes
				updateGridLayout(grid, showRoles, showIndices, showMetrics)
			case '3':
				showRoles = !showRoles
				updateGridLayout(grid, showRoles, showIndices, showMetrics)
			case '4':
				showIndices = !showIndices
				updateGridLayout(grid, showRoles, showIndices, showMetrics)
			case '5':
				showMetrics = !showMetrics
				updateGridLayout(grid, showRoles, showIndices, showMetrics)
			case 'h':
				showHiddenIndices = !showHiddenIndices
				// Don't call update() directly, just let the periodic update handle it
			}
		}
		return event
	})

	if err := app.SetRoot(grid, true).EnableMouse(true).Run(); err != nil {
		panic(err)
	}
}

func getTotalSegments(stats NodesStats) int64 {
	var total int64
	for _, node := range stats.Nodes {
		total += node.Indices.Segments.Count
	}
	return total
}

func getTotalOpenFiles(stats NodesStats) int64 {
	var total int64
	for _, node := range stats.Nodes {
		total += node.Process.OpenFileDescriptors
	}
	return total
}

func getTotalNetworkTX(stats NodesStats) int64 {
	var total int64
	for _, node := range stats.Nodes {
		total += node.Transport.TxSizeInBytes
	}
	return total
}

func getTotalNetworkRX(stats NodesStats) int64 {
	var total int64
	for _, node := range stats.Nodes {
		total += node.Transport.RxSizeInBytes
	}
	return total
}

func getMaxLengths(nodesInfo NodesInfo, indicesStats IndexStats) (int, int) {
	maxNodeNameLen := 0
	maxIndexNameLen := 0

	// Get max node name length
	for _, nodeInfo := range nodesInfo.Nodes {
		if len(nodeInfo.Name) > maxNodeNameLen {
			maxNodeNameLen = len(nodeInfo.Name)
		}
	}

	// Get max index name length
	for _, index := range indicesStats {
		// Consider hidden indices in max length calculation if they're shown
		if (showHiddenIndices || !strings.HasPrefix(index.Index, ".")) &&
			len(index.Index) > maxIndexNameLen {
			maxIndexNameLen = len(index.Index)
		}
	}

	maxNodeNameLen += 2  // Add padding
	maxIndexNameLen += 2 // Add padding

	return maxNodeNameLen, maxIndexNameLen
}

func getNodesPanelHeader(maxNodeNameLen int) string {
	return fmt.Sprintf("[::b]%-*s  [#444444]│[#00ffff] %-13s [#444444]│[#00ffff] %-20s [#444444]│[#00ffff] %-7s [#444444]│[#00ffff] %4s      [#444444]│[#00ffff] %4s [#444444]│[#00ffff] %-16s [#444444]│[#00ffff] %-16s [#444444]│[#00ffff] %-16s [#444444]│[#00ffff] %-25s[white]\n",
		maxNodeNameLen,
		"Node Name",
		"Roles",
		"Transport Address",
		"Version",
		"CPU",
		"Load",
		"Memory",
		"Heap",
		"Disk ",
		"OS")
}

func getIndicesPanelHeader(maxIndexNameLen int) string {
	return fmt.Sprintf("   [::b]%-*s  [#444444]│[#00ffff] %15s [#444444]│[#00ffff] %12s [#444444]│[#00ffff] %8s [#444444]│[#00ffff] %8s [#444444]│[#00ffff] %-12s [#444444]│[#00ffff] %-10s[white]\n",
		maxIndexNameLen,
		"Index Name",
		"Documents",
		"Size",
		"Shards",
		"Replicas",
		"Ingested",
		"Rate")
}
