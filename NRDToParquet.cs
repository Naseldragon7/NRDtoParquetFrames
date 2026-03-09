#region Using declarations
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;
using System.Xml.Linq;
using NinjaTrader.Cbi;
using NinjaTrader.Core;
using NinjaTrader.Data;
using NinjaTrader.Gui.Tools;
using NinjaTrader.NinjaScript;
#endregion

namespace NinjaTrader.Gui.NinjaScript
{
    public class NRDToCSV : AddOnBase
    {
        private NTMenuItem menuItem;
        private NTMenuItem existingMenuItemInControlCenter;

        protected override void OnStateChange()
        {
            if (State == State.SetDefaults)
            {
                Name = "NRDToParquet Enhanced";
                Description = "*.nrd to framed *.parquet market replay conversion (Enhanced v1.1.0)";
            }
        }

        protected override void OnWindowCreated(Window window)
        {
            ControlCenter cc = window as ControlCenter;
            if (cc == null)
                return;

            existingMenuItemInControlCenter = cc.FindFirst("ControlCenterMenuItemTools") as NTMenuItem;
            if (existingMenuItemInControlCenter == null)
                return;

            menuItem = new NTMenuItem
            {
                Header = "NRD to Parquet",
                Style = Application.Current.TryFindResource("MainMenuItem") as Style,
            };

            existingMenuItemInControlCenter.Items.Add(menuItem);
            menuItem.Click += OnMenuItemClick;
        }

        protected override void OnWindowDestroyed(Window window)
        {
            if (menuItem != null && window is ControlCenter)
            {
                if (existingMenuItemInControlCenter != null && existingMenuItemInControlCenter.Items.Contains(menuItem))
                    existingMenuItemInControlCenter.Items.Remove(menuItem);

                menuItem.Click -= OnMenuItemClick;
                menuItem = null;
            }
        }

        private void OnMenuItemClick(object sender, RoutedEventArgs e)
        {
            Core.Globals.RandomDispatcher.BeginInvoke(new Action(delegate
            {
                new NRDToCSVWindow().Show();
            }));
        }
    }

    public class NRDToCSVWindow : NTWindow, IWorkspacePersistence
    {
        private const int PARALLEL_THREADS_COUNT = 8;
        private const int DELAY_BETWEEN_FILES_MS = 50;
        private const long MIN_VALID_PARQUET_SIZE = 256;
        private const int FRAME_MS = 10;
        private const int BOOK_DEPTH = 10;
        private const long PRICE_SCALE = 100000;
        private const int FRAME_BATCH_ROWS = 250000;
        private const int MAX_ROWS_PER_FILE = 2000000;
        private const int CSV_STREAM_BUFFER_SIZE = 1048576;
        private const int CSV_READER_BUFFER_SIZE = 4194304;
        private const long NANOS_PER_MILLISECOND = 1000000L;
        private const long NANOS_PER_SECOND = 1000000000L;

        private static readonly TimeZoneInfo OUTPUT_TIME_ZONE = ResolveOutputTimeZone();

        private TextBox tbOutputRootDir;
        private TextBox tbSelectedInstruments;
        private Button bConvert;
        private TextBox tbOutput;
        private Label lProgress;
        private ProgressBar pbProgress;

        private int taskCount;
        private DateTime startTimestamp;
        private long completeFilesLength;
        private long totalFilesLength;
        private bool running;
        private volatile bool canceling;

        public NRDToCSVWindow()
        {
            Caption = "NRD to Parquet Enhanced v1.1.0";
            Width = 560;
            Height = 540;
            Content = BuildContent();

            Loaded += delegate
            {
                if (WorkspaceOptions == null)
                    WorkspaceOptions = new WorkspaceOptions("NRDToCSV-" + Guid.NewGuid().ToString("N"), this);
            };

            Closing += delegate
            {
                if (bConvert != null)
                    bConvert.Click -= OnConvertButtonClick;
            };
        }

        protected override void OnClosed(EventArgs e)
        {
            if (running)
                canceling = true;

            base.OnClosed(e);
        }

        public WorkspaceOptions WorkspaceOptions { get; set; }

        private DependencyObject BuildContent()
        {
            double margin = (double) FindResource("MarginBase");

            tbOutputRootDir = new TextBox
            {
                Margin = new Thickness(margin, 0, margin, margin),
                Text = Path.Combine(Globals.UserDataDir, "db", "replay.parquet"),
            };

            Label lOutputRootDir = new Label
            {
                Foreground = FindResource("FontLabelBrush") as Brush,
                Margin = new Thickness(margin, 0, margin, 0),
                Content = "Root directory of converted Parquet frames:",
            };

            tbSelectedInstruments = new TextBox { Margin = new Thickness(margin, 0, margin, margin) };

            Label lSelectedInstruments = new Label
            {
                Foreground = FindResource("FontLabelBrush") as Brush,
                Margin = new Thickness(margin, margin, margin, 0),
                Content = "Semicolon separated RegEx'es to filter *.nrd file names (keep empty to proceed all):",
            };

            bConvert = new Button { Margin = new Thickness(margin), IsDefault = true, Content = "_Convert" };
            bConvert.Click += OnConvertButtonClick;

            tbOutput = new TextBox
            {
                IsReadOnly = true,
                HorizontalScrollBarVisibility = ScrollBarVisibility.Auto,
                VerticalScrollBarVisibility = ScrollBarVisibility.Auto,
                Margin = new Thickness(margin),
            };

            pbProgress = new ProgressBar { Height = 0 };

            lProgress = new Label
            {
                Foreground = FindResource("FontLabelBrush") as Brush,
                Height = 0,
            };

            Grid grid = new Grid { Background = new SolidColorBrush(Colors.Transparent) };
            grid.RowDefinitions.Add(new RowDefinition { Height = GridLength.Auto });
            grid.RowDefinitions.Add(new RowDefinition { Height = GridLength.Auto });
            grid.RowDefinitions.Add(new RowDefinition { Height = GridLength.Auto });
            grid.RowDefinitions.Add(new RowDefinition { Height = GridLength.Auto });
            grid.RowDefinitions.Add(new RowDefinition { Height = GridLength.Auto });
            grid.RowDefinitions.Add(new RowDefinition { Height = new GridLength(1, GridUnitType.Star) });
            grid.RowDefinitions.Add(new RowDefinition { Height = GridLength.Auto });
            grid.RowDefinitions.Add(new RowDefinition { Height = GridLength.Auto });
            grid.RowDefinitions.Add(new RowDefinition { Height = GridLength.Auto });

            Grid.SetRow(lOutputRootDir, 0);
            Grid.SetRow(tbOutputRootDir, 1);
            Grid.SetRow(lSelectedInstruments, 2);
            Grid.SetRow(tbSelectedInstruments, 3);
            Grid.SetRow(bConvert, 4);
            Grid.SetRow(tbOutput, 5);
            Grid.SetRow(lProgress, 6);
            Grid.SetRow(pbProgress, 7);

            Label lFooter = new Label
            {
                Foreground = new SolidColorBrush(Colors.Gray),
                FontSize = 10,
                HorizontalAlignment = HorizontalAlignment.Right,
                Content = "Enhanced v1.1.0 - direct framed parquet output",
                Margin = new Thickness(0, 0, margin, margin),
            };
            Grid.SetRow(lFooter, 8);

            grid.Children.Add(lOutputRootDir);
            grid.Children.Add(tbOutputRootDir);
            grid.Children.Add(lSelectedInstruments);
            grid.Children.Add(tbSelectedInstruments);
            grid.Children.Add(bConvert);
            grid.Children.Add(tbOutput);
            grid.Children.Add(lProgress);
            grid.Children.Add(pbProgress);
            grid.Children.Add(lFooter);

            return grid;
        }

        private void OnConvertButtonClick(object sender, RoutedEventArgs e)
        {
            if (tbOutput == null)
                return;

            logout("Run conversion...");

            if (running)
            {
                if (!canceling)
                {
                    canceling = true;
                    logout("Canceling conversion...");
                    bConvert.IsEnabled = false;
                    bConvert.Content = "Canceling...";
                }

                return;
            }

            tbOutput.Clear();

            string nrdDir = Path.Combine(Globals.UserDataDir, "db", "replay");
            string outputRootDir = tbOutputRootDir.Text;

            List<Regex> selectedInstruments = null;
            if (!tbSelectedInstruments.Text.IsNullOrEmpty())
            {
                try
                {
                    string[] patterns = tbSelectedInstruments.Text.Split(';');
                    selectedInstruments = new List<Regex>(patterns.Length);
                    for (int i = 0; i < patterns.Length; i++)
                        selectedInstruments.Add(new Regex(patterns[i].Trim()));
                }
                catch (ArgumentException ex)
                {
                    logout(string.Format("ERROR: Invalid regular expression: {0}", ex.Message));
                    return;
                }
            }

            if (!Directory.Exists(nrdDir))
            {
                logout(string.Format("ERROR: The NRD root directory \"{0}\" not found", nrdDir));
                return;
            }

            string[] nrdSubDirs = Directory.GetDirectories(nrdDir);
            if (nrdSubDirs.Length == 0)
            {
                logout(string.Format("WARNING: The NRD root directory \"{0}\" is empty", nrdDir));
                return;
            }

            if (!Directory.Exists(outputRootDir))
            {
                try
                {
                    Directory.CreateDirectory(outputRootDir);
                }
                catch (Exception error)
                {
                    logout(string.Format("ERROR: Unable to create the output root directory \"{0}\": {1}", outputRootDir, error.Message));
                    return;
                }
            }

            Globals.RandomDispatcher.InvokeAsync(new Action(delegate
            {
                Interlocked.Exchange(ref completeFilesLength, 0);
                Interlocked.Exchange(ref totalFilesLength, 0);

                List<DumpEntry> entries = new List<DumpEntry>();
                foreach (string subDir in nrdSubDirs)
                    ProceedDirectory(entries, nrdDir, subDir, outputRootDir, selectedInstruments);

                if (entries.Count == 0)
                {
                    logout("No *.nrd files found to convert");
                    return;
                }

                logout(string.Format("Convert {0} files into {1}ms / depth {2} parquet frames...", entries.Count, FRAME_MS, BOOK_DEPTH));
                run(entries.Count);
                taskCount = Math.Min(PARALLEL_THREADS_COUNT, entries.Count);

                int chunkSize = (entries.Count + PARALLEL_THREADS_COUNT - 1) / PARALLEL_THREADS_COUNT;
                for (int i = 0; i < PARALLEL_THREADS_COUNT; i++)
                {
                    int start = i * chunkSize;
                    int count = Math.Min(chunkSize, entries.Count - start);
                    if (count <= 0)
                        continue;

                    List<DumpEntry> partition = entries.GetRange(start, count);
                    RunConversion(partition, entries.Count);
                }
            }));
        }

        private void ProceedDirectory(List<DumpEntry> entries, string nrdRoot, string nrdDir, string outputRootDir, List<Regex> selectedInstruments)
        {
            string[] fileEntries = Directory.GetFiles(nrdDir, "*.nrd");
            if (fileEntries.Length == 0)
            {
                logout(string.Format("WARNING: No *.nrd files found in \"{0}\" directory. Skipped", nrdDir));
                return;
            }

            foreach (string fileName in fileEntries)
            {
                string fullName = Path.GetFileName(Path.GetDirectoryName(fileName));
                string relativeName = GetPathRelativeToRoot(nrdRoot, fileName);

                if (selectedInstruments != null && !MatchesAnyInstrumentFilter(selectedInstruments, relativeName))
                    continue;

                Collection<Instrument> instruments = InstrumentList.GetInstruments(fullName);
                if (instruments.Count == 0)
                {
                    logout(string.Format("Unable to find an instrument named \"{0}\". Skipped", fullName));
                    continue;
                }

                if (instruments.Count > 1)
                {
                    logout(string.Format("More than one instrument identified for name \"{0}\". Skipped", fullName));
                    continue;
                }

                Instrument instrument = instruments[0];
                string name = Path.GetFileNameWithoutExtension(fileName);

                DateTime date;
                if (!DateTime.TryParseExact(name, "yyyyMMdd", CultureInfo.InvariantCulture, DateTimeStyles.None, out date))
                {
                    logout(string.Format("ERROR: Unable to parse date from filename \"{0}\". Expected format: yyyyMMdd", name));
                    continue;
                }

                string outputPartitionDir = GetOutputPartitionDirectory(outputRootDir, instrument.FullName, date);
                if (HasExistingParquetOutput(outputPartitionDir))
                {
                    logout(string.Format("Conversion \"{0}\" to \"{1}\" is done already. Skipped",
                        relativeName,
                        GetPathRelativeToRoot(outputRootDir, outputPartitionDir)));
                    continue;
                }

                long nrdFileLength = new FileInfo(fileName).Length;
                Interlocked.Add(ref totalFilesLength, nrdFileLength);

                entries.Add(new DumpEntry
                {
                    NrdLength = nrdFileLength,
                    Instrument = instrument,
                    Date = date,
                    OutputPartitionDir = outputPartitionDir,
                    FromName = relativeName,
                    ToName = GetPathRelativeToRoot(outputRootDir, outputPartitionDir),
                });
            }
        }

        private void RunConversion(List<DumpEntry> partition, int totalCount)
        {
            Globals.RandomDispatcher.InvokeAsync(new Action(delegate
            {
                try
                {
                    foreach (DumpEntry entry in partition)
                    {
                        if (canceling)
                            break;

                        ConvertNrd(entry);

                        long newCompleted = Interlocked.Add(ref completeFilesLength, entry.NrdLength);

                        Dispatcher.InvokeAsync(new Action(delegate
                        {
                            pbProgress.Value++;

                            string eta = string.Empty;
                            if (newCompleted > 0 && totalFilesLength > 0)
                            {
                                double progress = (double) newCompleted / totalFilesLength;
                                TimeSpan elapsed = DateTime.Now - startTimestamp;
                                TimeSpan estimatedTotal = TimeSpan.FromSeconds(elapsed.TotalSeconds / progress);
                                TimeSpan remaining = estimatedTotal - elapsed;
                                eta = string.Format(" ETA: {0:hh\\:mm\\:ss}", remaining);
                            }

                            lProgress.Content = string.Format("{0} of {1} files converted ({2} of {3}){4}",
                                pbProgress.Value,
                                totalCount,
                                ToBytes(newCompleted),
                                ToBytes(totalFilesLength),
                                eta);
                        }));

                        if (!canceling && DELAY_BETWEEN_FILES_MS > 0)
                            Thread.Sleep(DELAY_BETWEEN_FILES_MS);
                    }
                }
                finally
                {
                    if (Interlocked.Decrement(ref taskCount) == 0)
                    {
                        complete();
                        logout(canceling ? "Conversion canceled" : "Conversion complete");
                    }
                }
            }));
        }

        private void ConvertNrd(DumpEntry entry)
        {
            logout(string.Format("Conversion \"{0}\" to \"{1}\"...", entry.FromName, entry.ToName));

            string tempCsvPath = Path.Combine(Path.GetTempPath(), "nrd-to-parquet-" + Guid.NewGuid().ToString("N") + ".csv");
            string stagedOutputPartitionDir = entry.OutputPartitionDir + ".tmp-" + Guid.NewGuid().ToString("N");

            try
            {
                Directory.CreateDirectory(stagedOutputPartitionDir);

                MarketReplay.DumpMarketDepth(entry.Instrument, entry.Date, entry.Date, tempCsvPath);
                FrameConversionResult result = ConvertCsvToFrameParquet(tempCsvPath, stagedOutputPartitionDir);

                if (canceling)
                {
                    logout(string.Format("Conversion \"{0}\" canceled before publish", entry.FromName));
                    return;
                }

                PublishStagedPartition(stagedOutputPartitionDir, entry.OutputPartitionDir);

                logout(string.Format("Conversion \"{0}\" to \"{1}\" complete ({2} frames, {3} bad lines)",
                    entry.FromName,
                    entry.ToName,
                    result.FrameCount,
                    result.BadLineCount));
            }
            catch (OutOfMemoryException)
            {
                logout("CRITICAL ERROR: Out of memory. Canceling all conversions...");
                canceling = true;
            }
            catch (IOException ex)
            {
                logout(string.Format("ERROR: File I/O error converting \"{0}\": {1}", entry.FromName, ex.Message));
            }
            catch (UnauthorizedAccessException ex)
            {
                logout(string.Format("ERROR: Access denied for \"{0}\": {1}", entry.ToName, ex.Message));
            }
            catch (Exception error)
            {
                logout(string.Format("ERROR: Conversion \"{0}\" to \"{1}\" failed: {2}", entry.FromName, entry.ToName, error.Message));
            }
            finally
            {
                TryDeleteFile(tempCsvPath);
                TryDeleteDirectory(stagedOutputPartitionDir);
            }
        }

        private FrameConversionResult ConvertCsvToFrameParquet(string csvPath, string outputPartitionDir)
        {
            long frameNs = FRAME_MS * NANOS_PER_MILLISECOND;

            long lastBaseTimestamp = -1;
            long lastEpochSeconds = 0;
            long sequence = 0;
            long totalLines = 0;
            long badLines = 0;
            long totalFrames = 0;

            bool initialized = false;
            long currentBucket = 0;
            long lastEventSeqInBucket = 0;
            bool dirty = false;
            bool hadEventInBucket = false;
            int eventCountInBucket = 0;

            int part = 0;
            int rowsInFile = 0;
            AtomicParquetWriter writer = null;
            Array schema = BuildFrameSchema(BOOK_DEPTH);
            OrderBook book = new OrderBook(BOOK_DEPTH);
            L1Snapshot l1 = new L1Snapshot();
            FrameBuffer buffer = new FrameBuffer(BOOK_DEPTH, FRAME_BATCH_ROWS);
            FrameSnapshot gapSnapshot = new FrameSnapshot(BOOK_DEPTH);
            int[] fieldStarts = new int[9];
            int[] fieldLengths = new int[9];

            using (FileStream stream = new FileStream(csvPath, FileMode.Open, FileAccess.Read, FileShare.Read, CSV_STREAM_BUFFER_SIZE, FileOptions.SequentialScan))
            using (StreamReader reader = new StreamReader(stream, Encoding.UTF8, true, CSV_READER_BUFFER_SIZE))
            {
                string line;

                while (!canceling && (line = reader.ReadLine()) != null)
                {
                    if (line.Length == 0)
                        continue;

                    totalLines++;
                    sequence++;

                    int kind;
                    if (!TryParseKind(line, out kind))
                    {
                        badLines++;
                        continue;
                    }

                    ParsedEvent parsedEvent;
                    if (!TryParseEvent(line, kind, sequence, ref lastBaseTimestamp, ref lastEpochSeconds, fieldStarts, fieldLengths, out parsedEvent))
                    {
                        badLines++;
                        continue;
                    }

                    if (!initialized)
                    {
                        currentBucket = FloorDiv(parsedEvent.TimestampNanos, frameNs);
                        initialized = true;
                    }

                    long eventBucket = FloorDiv(parsedEvent.TimestampNanos, frameNs);
                    while (eventBucket > currentBucket)
                    {
                        EmitFrame(buffer, book, l1, currentBucket, lastEventSeqInBucket, hadEventInBucket, eventCountInBucket, frameNs);

                        FlushBufferedFrames(outputPartitionDir, schema, buffer, ref writer, ref part, ref rowsInFile, ref totalFrames, false);

                        dirty = false;
                        hadEventInBucket = false;
                        eventCountInBucket = 0;
                        lastEventSeqInBucket = 0;
                        currentBucket++;

                        long gapBucketCount = eventBucket - currentBucket;
                        if (gapBucketCount > 0)
                        {
                            CaptureFrameSnapshot(gapSnapshot, book, l1);
                            AppendGapFrames(outputPartitionDir, schema, buffer, ref writer, ref part, ref rowsInFile, ref totalFrames, gapSnapshot, currentBucket, gapBucketCount, frameNs);
                            currentBucket += gapBucketCount;
                        }
                    }

                    if (parsedEvent.Kind == 1)
                        ApplyL1(l1, parsedEvent);
                    else
                        ApplyL2(book, parsedEvent);

                    dirty = true;
                    hadEventInBucket = true;
                    eventCountInBucket++;
                    lastEventSeqInBucket = parsedEvent.Sequence;
                }
            }

            try
            {
                if (initialized && !canceling && dirty)
                {
                    EmitFrame(buffer, book, l1, currentBucket, lastEventSeqInBucket, hadEventInBucket, eventCountInBucket, frameNs);
                }

                if (buffer.Count > 0)
                    FlushBufferedFrames(outputPartitionDir, schema, buffer, ref writer, ref part, ref rowsInFile, ref totalFrames, true);

                if (writer != null)
                {
                    CloseWriterAtomic(writer);
                    writer = null;
                }
            }
            finally
            {
                if (writer != null)
                    writer.Dispose();
            }

            if (canceling)
                return new FrameConversionResult(totalLines, badLines, totalFrames);

            if (totalLines > 0 && totalFrames == 0 && badLines == totalLines)
                throw new InvalidDataException("CSV contained lines but none could be parsed into frames.");
            if (totalFrames == 0)
                throw new InvalidDataException("No parquet frames were generated from the dumped CSV.");

            return new FrameConversionResult(totalLines, badLines, totalFrames);
        }

        private static bool TryParseEvent(string line, int kind, long sequence, ref long lastBaseTimestamp, ref long lastEpochSeconds, int[] starts, int[] lengths, out ParsedEvent parsedEvent)
        {
            parsedEvent = new ParsedEvent();

            int fieldCount = kind == 1 ? 6 : 9;
            if (SplitSemicolon(line, starts, lengths, fieldCount) != fieldCount)
                return false;

            short type;
            long baseTimestamp;
            long offsetNs;
            long priceI64;
            int volume;

            if (!TryParseInt16Field(line, starts[1], lengths[1], out type))
                return false;
            if (!TryParseInt64Field(line, starts[2], lengths[2], out baseTimestamp))
                return false;
            if (!TryParseInt64Field(line, starts[3], lengths[3], out offsetNs))
                return false;

            long epochSeconds;
            if (baseTimestamp == lastBaseTimestamp)
            {
                epochSeconds = lastEpochSeconds;
            }
            else
            {
                epochSeconds = ToEpochSecondsFromBaseYmdHms(baseTimestamp, OUTPUT_TIME_ZONE);
                lastBaseTimestamp = baseTimestamp;
                lastEpochSeconds = epochSeconds;
            }

            long timestampNanos = checked(epochSeconds * NANOS_PER_SECOND + offsetNs);

            if (kind == 1)
            {
                if (!TryParseScaledPriceI64(line, starts[4], lengths[4], PRICE_SCALE, out priceI64))
                    return false;
                if (!TryParseInt32Field(line, starts[5], lengths[5], out volume))
                    return false;

                parsedEvent = ParsedEvent.CreateL1(timestampNanos, sequence, type, priceI64, volume);
                return true;
            }

            short cbiOp;
            short position;

            if (!TryParseInt16Field(line, starts[4], lengths[4], out cbiOp))
                return false;
            if (!TryParseInt16Field(line, starts[5], lengths[5], out position))
                return false;
            if (!TryParseScaledPriceI64(line, starts[7], lengths[7], PRICE_SCALE, out priceI64))
                return false;
            if (!TryParseInt32Field(line, starts[8], lengths[8], out volume))
                return false;

            parsedEvent = ParsedEvent.CreateL2(timestampNanos, sequence, type, cbiOp, position, priceI64, volume);
            return true;
        }

        private static void EmitFrame(FrameBuffer buffer, OrderBook book, L1Snapshot l1, long bucket, long lastEventSeqInBucket, bool hadEventInBucket, int eventCountInBucket, long frameNs)
        {
            long tsFrame = checked((bucket + 1) * frameNs);

            BestLevel bestBid = book.GetBestBid();
            BestLevel bestAsk = book.GetBestAsk();

            long midPx = 0;
            long spread = 0;
            if (bestBid.Price > 0 && bestAsk.Price > 0 && bestAsk.Price >= bestBid.Price)
            {
                spread = bestAsk.Price - bestBid.Price;
                midPx = bestBid.Price + spread / 2;
            }

            float imbalanceTop1 = 0f;
            int topDenominator = bestBid.Size + bestAsk.Size;
            if (topDenominator > 0)
                imbalanceTop1 = (float) (bestBid.Size - bestAsk.Size) / topDenominator;

            buffer.Append(
                tsFrame,
                lastEventSeqInBucket,
                hadEventInBucket ? 1 : 0,
                eventCountInBucket,
                bestBid.Price,
                bestBid.Size,
                bestAsk.Price,
                bestAsk.Size,
                midPx,
                spread,
                imbalanceTop1,
                book.ImbalanceTopN(),
                book,
                l1);
        }

        private static void CaptureFrameSnapshot(FrameSnapshot snapshot, OrderBook book, L1Snapshot l1)
        {
            BestLevel bestBid = book.GetBestBid();
            BestLevel bestAsk = book.GetBestAsk();

            long midPx = 0;
            long spread = 0;
            if (bestBid.Price > 0 && bestAsk.Price > 0 && bestAsk.Price >= bestBid.Price)
            {
                spread = bestAsk.Price - bestBid.Price;
                midPx = bestBid.Price + spread / 2;
            }

            float imbalanceTop1 = 0f;
            int topDenominator = bestBid.Size + bestAsk.Size;
            if (topDenominator > 0)
                imbalanceTop1 = (float) (bestBid.Size - bestAsk.Size) / topDenominator;

            snapshot.BestBidPx = bestBid.Price;
            snapshot.BestBidSz = bestBid.Size;
            snapshot.BestAskPx = bestAsk.Price;
            snapshot.BestAskSz = bestAsk.Size;
            snapshot.MidPx = midPx;
            snapshot.Spread = spread;
            snapshot.ImbalanceTop1 = imbalanceTop1;
            snapshot.ImbalanceTopN = book.ImbalanceTopN();

            Array.Copy(book.BidPrices, snapshot.BidPx, snapshot.Depth);
            Array.Copy(book.BidSizes, snapshot.BidSz, snapshot.Depth);
            Array.Copy(book.AskPrices, snapshot.AskPx, snapshot.Depth);
            Array.Copy(book.AskSizes, snapshot.AskSz, snapshot.Depth);

            snapshot.L1BidPx = l1.BidPx;
            snapshot.L1BidSz = l1.BidSz;
            snapshot.L1AskPx = l1.AskPx;
            snapshot.L1AskSz = l1.AskSz;
            snapshot.L1LastPx = l1.LastPx;
            snapshot.L1LastSz = l1.LastSz;
            snapshot.L1DailyVolume = l1.DailyVolume;
        }

        private static void AppendGapFrames(string outputPartitionDir, Array schema, FrameBuffer buffer, ref AtomicParquetWriter writer, ref int part, ref int rowsInFile, ref long totalFrames, FrameSnapshot snapshot, long startBucket, long gapBucketCount, long frameNs)
        {
            long bucket = startBucket;
            long remaining = gapBucketCount;

            while (remaining > 0)
            {
                int frameCount = (int) Math.Min(buffer.RemainingCapacity, remaining);
                if (frameCount == 0)
                {
                    FlushBufferedFrames(outputPartitionDir, schema, buffer, ref writer, ref part, ref rowsInFile, ref totalFrames, false);
                    continue;
                }

                buffer.AppendRepeated(bucket, frameNs, frameCount, snapshot);
                bucket += frameCount;
                remaining -= frameCount;

                if (buffer.Count == buffer.Capacity)
                    FlushBufferedFrames(outputPartitionDir, schema, buffer, ref writer, ref part, ref rowsInFile, ref totalFrames, false);
            }
        }

        private static void FlushBufferedFrames(string outputPartitionDir, Array schema, FrameBuffer buffer, ref AtomicParquetWriter writer, ref int part, ref int rowsInFile, ref long totalFrames, bool force)
        {
            if (buffer.Count == 0)
                return;

            if (!force && buffer.Count < buffer.Capacity)
                return;

            if (writer == null)
                writer = OpenWriterAtomic(Path.Combine(outputPartitionDir, string.Format("part-{0:D4}.parquet", part)), schema);

            FlushFrames(writer.Writer, buffer);
            rowsInFile += buffer.Count;
            totalFrames += buffer.Count;
            buffer.Reset();

            if (rowsInFile >= MAX_ROWS_PER_FILE)
            {
                CloseWriterAtomic(writer);
                writer = null;
                part++;
                rowsInFile = 0;
            }
        }

        private static void ApplyL2(OrderBook book, ParsedEvent parsedEvent)
        {
            int side = parsedEvent.Type;
            int operation = parsedEvent.Operation;
            int position = parsedEvent.Position;

            if ((uint) position >= (uint) book.Depth)
                return;

            if (operation == 2)
            {
                book.SetLevel(side, position, book.GetPrice(side, position), 0);
                return;
            }

            book.SetLevel(side, position, parsedEvent.PriceI64, parsedEvent.Volume);
        }

        private static void ApplyL1(L1Snapshot snapshot, ParsedEvent parsedEvent)
        {
            switch (parsedEvent.Type)
            {
                case 0:
                    snapshot.AskPx = parsedEvent.PriceI64;
                    snapshot.AskSz = parsedEvent.Volume;
                    break;

                case 1:
                    snapshot.BidPx = parsedEvent.PriceI64;
                    snapshot.BidSz = parsedEvent.Volume;
                    break;

                case 2:
                    snapshot.LastPx = parsedEvent.PriceI64;
                    snapshot.LastSz = parsedEvent.Volume;
                    break;

                case 5:
                    snapshot.DailyVolume = parsedEvent.Volume;
                    break;
            }
        }

        private static Array BuildFrameSchema(int depth)
        {
            List<object> columns = new List<object>(64 + depth * 4);

            columns.Add(ParquetDynamic.CreateColumn(typeof(long), "ts_frame_nanos"));
            columns.Add(ParquetDynamic.CreateColumn(typeof(long), "last_event_seq"));
            columns.Add(ParquetDynamic.CreateColumn(typeof(int), "had_event"));
            columns.Add(ParquetDynamic.CreateColumn(typeof(int), "event_count"));

            columns.Add(ParquetDynamic.CreateColumn(typeof(long), "best_bid_px_i64"));
            columns.Add(ParquetDynamic.CreateColumn(typeof(int), "best_bid_sz"));
            columns.Add(ParquetDynamic.CreateColumn(typeof(long), "best_ask_px_i64"));
            columns.Add(ParquetDynamic.CreateColumn(typeof(int), "best_ask_sz"));

            columns.Add(ParquetDynamic.CreateColumn(typeof(long), "mid_px_i64"));
            columns.Add(ParquetDynamic.CreateColumn(typeof(long), "spread_i64"));

            columns.Add(ParquetDynamic.CreateColumn(typeof(float), "imbalance_top1"));
            columns.Add(ParquetDynamic.CreateColumn(typeof(float), "imbalance_topN"));

            for (int i = 0; i < depth; i++)
                columns.Add(ParquetDynamic.CreateColumn(typeof(long), "bid_px_i64_" + i));
            for (int i = 0; i < depth; i++)
                columns.Add(ParquetDynamic.CreateColumn(typeof(int), "bid_sz_" + i));
            for (int i = 0; i < depth; i++)
                columns.Add(ParquetDynamic.CreateColumn(typeof(long), "ask_px_i64_" + i));
            for (int i = 0; i < depth; i++)
                columns.Add(ParquetDynamic.CreateColumn(typeof(int), "ask_sz_" + i));

            columns.Add(ParquetDynamic.CreateColumn(typeof(long), "l1_bid_px_i64"));
            columns.Add(ParquetDynamic.CreateColumn(typeof(int), "l1_bid_sz"));
            columns.Add(ParquetDynamic.CreateColumn(typeof(long), "l1_ask_px_i64"));
            columns.Add(ParquetDynamic.CreateColumn(typeof(int), "l1_ask_sz"));
            columns.Add(ParquetDynamic.CreateColumn(typeof(long), "l1_last_px_i64"));
            columns.Add(ParquetDynamic.CreateColumn(typeof(int), "l1_last_sz"));
            columns.Add(ParquetDynamic.CreateColumn(typeof(int), "l1_daily_volume"));

            return ParquetDynamic.CreateSchemaArray(columns);
        }

        private static AtomicParquetWriter OpenWriterAtomic(string finalPath, Array schema)
        {
            string tmpPath = finalPath + ".tmp";

            if (File.Exists(tmpPath))
                File.Delete(tmpPath);
            if (File.Exists(finalPath))
                File.Delete(finalPath);

            object writer = ParquetDynamic.CreateWriter(tmpPath, schema);
            return new AtomicParquetWriter(tmpPath, finalPath, writer);
        }

        private static void CloseWriterAtomic(AtomicParquetWriter writer)
        {
            ParquetDynamic.CloseWriter(writer.Writer);

            if (File.Exists(writer.FinalPath))
                File.Delete(writer.FinalPath);

            File.Move(writer.TmpPath, writer.FinalPath);
            writer.Dispose();
        }

        private static void FlushFrames(object writer, FrameBuffer buffer)
        {
            using (IDisposable rowGroup = ParquetDynamic.AppendRowGroup(writer))
            {
                ParquetDynamic.WriteBatch(rowGroup, typeof(long), buffer.TsFrameNanos, buffer.Count);
                ParquetDynamic.WriteBatch(rowGroup, typeof(long), buffer.LastEventSeq, buffer.Count);
                ParquetDynamic.WriteBatch(rowGroup, typeof(int), buffer.HadEvent, buffer.Count);
                ParquetDynamic.WriteBatch(rowGroup, typeof(int), buffer.EventCount, buffer.Count);

                ParquetDynamic.WriteBatch(rowGroup, typeof(long), buffer.BestBidPx, buffer.Count);
                ParquetDynamic.WriteBatch(rowGroup, typeof(int), buffer.BestBidSz, buffer.Count);
                ParquetDynamic.WriteBatch(rowGroup, typeof(long), buffer.BestAskPx, buffer.Count);
                ParquetDynamic.WriteBatch(rowGroup, typeof(int), buffer.BestAskSz, buffer.Count);

                ParquetDynamic.WriteBatch(rowGroup, typeof(long), buffer.MidPx, buffer.Count);
                ParquetDynamic.WriteBatch(rowGroup, typeof(long), buffer.Spread, buffer.Count);

                ParquetDynamic.WriteBatch(rowGroup, typeof(float), buffer.ImbalanceTop1, buffer.Count);
                ParquetDynamic.WriteBatch(rowGroup, typeof(float), buffer.ImbalanceTopN, buffer.Count);

                for (int i = 0; i < buffer.Depth; i++)
                    ParquetDynamic.WriteBatch(rowGroup, typeof(long), buffer.BidPx[i], buffer.Count);
                for (int i = 0; i < buffer.Depth; i++)
                    ParquetDynamic.WriteBatch(rowGroup, typeof(int), buffer.BidSz[i], buffer.Count);
                for (int i = 0; i < buffer.Depth; i++)
                    ParquetDynamic.WriteBatch(rowGroup, typeof(long), buffer.AskPx[i], buffer.Count);
                for (int i = 0; i < buffer.Depth; i++)
                    ParquetDynamic.WriteBatch(rowGroup, typeof(int), buffer.AskSz[i], buffer.Count);

                ParquetDynamic.WriteBatch(rowGroup, typeof(long), buffer.L1BidPx, buffer.Count);
                ParquetDynamic.WriteBatch(rowGroup, typeof(int), buffer.L1BidSz, buffer.Count);
                ParquetDynamic.WriteBatch(rowGroup, typeof(long), buffer.L1AskPx, buffer.Count);
                ParquetDynamic.WriteBatch(rowGroup, typeof(int), buffer.L1AskSz, buffer.Count);
                ParquetDynamic.WriteBatch(rowGroup, typeof(long), buffer.L1LastPx, buffer.Count);
                ParquetDynamic.WriteBatch(rowGroup, typeof(int), buffer.L1LastSz, buffer.Count);
                ParquetDynamic.WriteBatch(rowGroup, typeof(int), buffer.L1DailyVolume, buffer.Count);
            }
        }

        private static bool TryParseKind(string line, out int kind)
        {
            kind = 0;

            if (line.Length < 3)
                return false;

            if (line[0] != 'L' || line[2] != ';')
                return false;

            if (line[1] == '1')
            {
                kind = 1;
                return true;
            }

            if (line[1] == '2')
            {
                kind = 2;
                return true;
            }

            return false;
        }

        private static int SplitSemicolon(string line, int[] starts, int[] lengths, int expectedFields)
        {
            int field = 0;
            int start = 0;

            for (int i = 0; i <= line.Length; i++)
            {
                if (i == line.Length || line[i] == ';')
                {
                    if (field >= expectedFields)
                        return field + 1;

                    starts[field] = start;
                    lengths[field] = i - start;
                    field++;
                    start = i + 1;
                }
            }

            return field;
        }

        private static bool TryParseInt64Field(string value, int start, int length, out long parsed)
        {
            parsed = 0;
            if (length <= 0)
                return false;

            int index = start;
            int end = start + length;
            bool negative = false;

            if (value[index] == '-')
            {
                negative = true;
                index++;
                if (index >= end)
                    return false;
            }

            long result = 0;
            while (index < end)
            {
                int digit = value[index] - '0';
                if ((uint) digit > 9)
                    return false;

                result = checked(result * 10 + digit);
                index++;
            }

            parsed = negative ? -result : result;
            return true;
        }

        private static bool TryParseInt32Field(string value, int start, int length, out int parsed)
        {
            parsed = 0;

            long result;
            if (!TryParseInt64Field(value, start, length, out result))
                return false;
            if (result < int.MinValue || result > int.MaxValue)
                return false;

            parsed = (int) result;
            return true;
        }

        private static bool TryParseInt16Field(string value, int start, int length, out short parsed)
        {
            parsed = 0;

            long result;
            if (!TryParseInt64Field(value, start, length, out result))
                return false;
            if (result < short.MinValue || result > short.MaxValue)
                return false;

            parsed = (short) result;
            return true;
        }

        private static bool TryParseScaledPriceI64(string value, int start, int length, long priceScale, out long priceI64)
        {
            priceI64 = 0;
            if (length <= 0)
                return false;

            string priceText = value.Substring(start, length);
            decimal price;

            if (!decimal.TryParse(priceText, NumberStyles.Number, CultureInfo.InvariantCulture, out price) &&
                !decimal.TryParse(priceText, NumberStyles.Number, CultureInfo.CurrentCulture, out price))
            {
                return false;
            }

            decimal scaled = price * priceScale;
            priceI64 = (long) decimal.Round(scaled, 0, MidpointRounding.AwayFromZero);
            return true;
        }

        private static long ToEpochSecondsFromBaseYmdHms(long ymdHms, TimeZoneInfo timeZone)
        {
            long value = ymdHms;
            int second = (int) (value % 100);
            value /= 100;
            int minute = (int) (value % 100);
            value /= 100;
            int hour = (int) (value % 100);
            value /= 100;
            int day = (int) (value % 100);
            value /= 100;
            int month = (int) (value % 100);
            value /= 100;
            int year = (int) value;

            DateTime local = new DateTime(year, month, day, hour, minute, second, DateTimeKind.Unspecified);

            if (timeZone.IsInvalidTime(local))
                local = local.AddHours(1);

            if (timeZone.IsAmbiguousTime(local))
            {
                TimeSpan[] offsets = timeZone.GetAmbiguousTimeOffsets(local);
                TimeSpan chosenOffset = offsets[0];
                for (int i = 1; i < offsets.Length; i++)
                {
                    if (offsets[i] > chosenOffset)
                        chosenOffset = offsets[i];
                }
                DateTimeOffset ambiguous = new DateTimeOffset(local, chosenOffset);
                return ambiguous.ToUnixTimeSeconds();
            }

            DateTime utc = TimeZoneInfo.ConvertTimeToUtc(local, timeZone);
            return new DateTimeOffset(utc).ToUnixTimeSeconds();
        }

        private static long FloorDiv(long dividend, long divisor)
        {
            long quotient = dividend / divisor;
            long remainder = dividend % divisor;
            if (remainder < 0)
                quotient--;
            return quotient;
        }

        private static string GetOutputPartitionDirectory(string outputRootDir, string instrumentName, DateTime date)
        {
            return Path.Combine(
                outputRootDir,
                "data",
                "frames",
                "frame_ms=" + FRAME_MS,
                "depth=" + BOOK_DEPTH,
                "instrument=" + SanitizePartition(instrumentName),
                "date=" + date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture));
        }

        private static string SanitizePartition(string value)
        {
            return value
                .Replace(Path.DirectorySeparatorChar, '_')
                .Replace(Path.AltDirectorySeparatorChar, '_');
        }

        private static bool MatchesAnyInstrumentFilter(List<Regex> filters, string relativeName)
        {
            for (int i = 0; i < filters.Count; i++)
            {
                if (filters[i].IsMatch(relativeName))
                    return true;
            }

            return false;
        }

        private static string GetPathRelativeToRoot(string rootPath, string path)
        {
            string normalizedRoot = rootPath.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
            if (normalizedRoot.Length == 0)
                return path;

            string prefix = normalizedRoot + Path.DirectorySeparatorChar;
            if (path.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
                return path.Substring(prefix.Length);

            prefix = normalizedRoot + Path.AltDirectorySeparatorChar;
            if (path.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
                return path.Substring(prefix.Length);

            return path;
        }

        private static bool HasExistingParquetOutput(string outputPartitionDir)
        {
            if (!Directory.Exists(outputPartitionDir))
                return false;

            foreach (string fileName in Directory.GetFiles(outputPartitionDir, "part-*.parquet"))
            {
                FileInfo fileInfo = new FileInfo(fileName);
                if (fileInfo.Length >= MIN_VALID_PARQUET_SIZE)
                    return true;
            }

            return false;
        }

        private static void PublishStagedPartition(string stagedOutputPartitionDir, string outputPartitionDir)
        {
            string parentDir = Path.GetDirectoryName(outputPartitionDir);
            if (!string.IsNullOrEmpty(parentDir) && !Directory.Exists(parentDir))
                Directory.CreateDirectory(parentDir);

            string backupOutputPartitionDir = outputPartitionDir + ".bak-" + Guid.NewGuid().ToString("N");

            try
            {
                if (Directory.Exists(outputPartitionDir))
                    Directory.Move(outputPartitionDir, backupOutputPartitionDir);

                Directory.Move(stagedOutputPartitionDir, outputPartitionDir);
            }
            catch
            {
                if (!Directory.Exists(outputPartitionDir) && Directory.Exists(backupOutputPartitionDir))
                    Directory.Move(backupOutputPartitionDir, outputPartitionDir);

                throw;
            }
            finally
            {
                if (Directory.Exists(outputPartitionDir) && Directory.Exists(backupOutputPartitionDir))
                    TryDeleteDirectory(backupOutputPartitionDir);
            }
        }

        private static void TryDeleteFile(string fileName)
        {
            try
            {
                if (File.Exists(fileName))
                    File.Delete(fileName);
            }
            catch
            {
            }
        }

        private static void TryDeleteDirectory(string directoryName)
        {
            try
            {
                if (Directory.Exists(directoryName))
                    Directory.Delete(directoryName, true);
            }
            catch
            {
            }
        }

        private static TimeZoneInfo ResolveOutputTimeZone()
        {
            string[] ids = { "Eastern Standard Time", "America/New_York" };

            foreach (string id in ids)
            {
                try
                {
                    return TimeZoneInfo.FindSystemTimeZoneById(id);
                }
                catch
                {
                }
            }

            return TimeZoneInfo.Local;
        }

        public void Restore(XDocument document, XElement element)
        {
            foreach (XElement elRoot in element.Elements())
            {
                if (!elRoot.Name.LocalName.Contains("NRDToCSV"))
                    continue;

                XElement elOutputRootDir = elRoot.Element("OutputRootDir");
                if (elOutputRootDir == null)
                    elOutputRootDir = elRoot.Element("CsvRootDir");
                if (elOutputRootDir != null)
                    tbOutputRootDir.Text = elOutputRootDir.Value;

                XElement elSelectedInstruments = elRoot.Element("SelectedInstruments");
                if (elSelectedInstruments != null)
                    tbSelectedInstruments.Text = elSelectedInstruments.Value;
            }
        }

        public void Save(XDocument document, XElement element)
        {
            element.Elements().Where(delegate(XElement el) { return el.Name.LocalName.Equals("NRDToCSV"); }).Remove();

            XElement elRoot = new XElement("NRDToCSV");
            XElement elOutputRootDir = new XElement("OutputRootDir", tbOutputRootDir.Text);
            XElement elSelectedInstruments = new XElement("SelectedInstruments", tbSelectedInstruments.Text);

            elRoot.Add(elOutputRootDir);
            elRoot.Add(elSelectedInstruments);
            element.Add(elRoot);
        }

        private void logout(string text)
        {
            Dispatcher.InvokeAsync(new Action(delegate
            {
                tbOutput.AppendText(text + Environment.NewLine);
                tbOutput.ScrollToEnd();
            }));
        }

        private void run(int filesCount)
        {
            Dispatcher.InvokeAsync(new Action(delegate
            {
                running = true;
                canceling = false;
                bConvert.IsEnabled = true;
                bConvert.Content = "_Cancel";
                tbOutputRootDir.IsReadOnly = true;
                tbSelectedInstruments.IsReadOnly = true;

                double margin = (double) FindResource("MarginBase");
                lProgress.Height = 24;
                pbProgress.Margin = new Thickness(margin, 0, margin, margin);
                pbProgress.Height = 16;
                pbProgress.Minimum = 0;
                pbProgress.Maximum = filesCount;
                pbProgress.Value = 0;
                startTimestamp = DateTime.Now;
            }));
        }

        private void complete()
        {
            Dispatcher.InvokeAsync(new Action(delegate
            {
                running = false;
                lProgress.Margin = new Thickness(0);
                lProgress.Height = 0;
                pbProgress.Margin = new Thickness(0);
                pbProgress.Height = 0;
                tbOutputRootDir.IsReadOnly = false;
                tbSelectedInstruments.IsReadOnly = false;
                bConvert.IsEnabled = true;
                bConvert.Content = "_Convert";
            }));
        }

        public static string ToBytes(long bytes)
        {
            if (bytes < 1024)
                return string.Format("{0} B", bytes);

            double exp = (int) (Math.Log(bytes) / Math.Log(1024));
            return string.Format("{0:F1} {1}iB", bytes / Math.Pow(1024, exp), "KMGTPE"[(int) exp - 1]);
        }

        private static class ParquetDynamic
        {
            private static readonly object sync = new object();
            private static Assembly assembly;
            private static Type columnBaseType;
            private static Type columnGenericType;
            private static Type writerType;
            private static Type compressionType;
            private static MethodInfo appendRowGroupMethod;
            private static MethodInfo closeWriterMethod;
            private static readonly Dictionary<Type, MethodInfo> logicalWriterMethods = new Dictionary<Type, MethodInfo>();
            private static readonly Dictionary<Type, MethodInfo> writeBatchMethods = new Dictionary<Type, MethodInfo>();

            public static object CreateColumn(Type elementType, string name)
            {
                EnsureLoaded();
                Type closedColumnType = columnGenericType.MakeGenericType(elementType);
                ConstructorInfo[] ctors = closedColumnType.GetConstructors();
                for (int i = 0; i < ctors.Length; i++)
                {
                    ConstructorInfo ctor = ctors[i];
                    ParameterInfo[] parameters = ctor.GetParameters();
                    if (parameters.Length == 0)
                        continue;
                    if (parameters[0].ParameterType != typeof(string))
                        continue;

                    object[] args = new object[parameters.Length];
                    args[0] = name;

                    bool supported = true;
                    for (int p = 1; p < parameters.Length; p++)
                    {
                        ParameterInfo parameter = parameters[p];
                        if (parameter.IsOptional)
                        {
                            object defaultValue = parameter.DefaultValue;
                            if (defaultValue == DBNull.Value || defaultValue == Missing.Value)
                                defaultValue = GetDefaultValue(parameter.ParameterType);
                            args[p] = defaultValue;
                            continue;
                        }

                        args[p] = GetDefaultValue(parameter.ParameterType);
                        if (args[p] == null && parameter.ParameterType.IsValueType && Nullable.GetUnderlyingType(parameter.ParameterType) == null)
                        {
                            supported = false;
                            break;
                        }
                    }

                    if (supported)
                        return ctor.Invoke(args);
                }

                throw new InvalidOperationException("Unable to construct ParquetSharp column for type " + elementType.FullName);
            }

            public static Array CreateSchemaArray(List<object> columns)
            {
                EnsureLoaded();
                Array schema = Array.CreateInstance(columnBaseType, columns.Count);
                for (int i = 0; i < columns.Count; i++)
                    schema.SetValue(columns[i], i);
                return schema;
            }

            public static object CreateWriter(string path, Array schema)
            {
                EnsureLoaded();

                ConstructorInfo[] ctors = writerType.GetConstructors();
                for (int i = 0; i < ctors.Length; i++)
                {
                    ConstructorInfo ctor = ctors[i];
                    ParameterInfo[] parameters = ctor.GetParameters();
                    if (parameters.Length < 2)
                        continue;
                    if (parameters[0].ParameterType != typeof(string))
                        continue;
                    if (!parameters[1].ParameterType.IsAssignableFrom(schema.GetType()))
                        continue;

                    object[] args = new object[parameters.Length];
                    args[0] = path;
                    args[1] = schema;

                    bool supported = true;
                    for (int p = 2; p < parameters.Length; p++)
                    {
                        ParameterInfo parameter = parameters[p];
                        object value;

                        if (compressionType != null && parameter.ParameterType == compressionType)
                        {
                            value = TryGetCompressionValue();
                            if (value == null)
                            {
                                supported = false;
                                break;
                            }
                        }
                        else if (parameter.IsOptional)
                        {
                            value = parameter.DefaultValue;
                            if (value == DBNull.Value || value == Missing.Value)
                                value = GetDefaultValue(parameter.ParameterType);
                        }
                        else
                        {
                            value = GetDefaultValue(parameter.ParameterType);
                            if (value == null && parameter.ParameterType.IsValueType && Nullable.GetUnderlyingType(parameter.ParameterType) == null)
                            {
                                supported = false;
                                break;
                            }
                        }

                        args[p] = value;
                    }

                    if (supported)
                        return ctor.Invoke(args);
                }

                throw new InvalidOperationException("Unable to find a supported ParquetFileWriter constructor. Check the installed ParquetSharp version.");
            }

            public static IDisposable AppendRowGroup(object writer)
            {
                EnsureLoaded();
                return (IDisposable) appendRowGroupMethod.Invoke(writer, null);
            }

            public static void CloseWriter(object writer)
            {
                EnsureLoaded();
                closeWriterMethod.Invoke(writer, null);
            }

            public static void DisposeWriter(object writer)
            {
                IDisposable disposable = writer as IDisposable;
                if (disposable != null)
                    disposable.Dispose();
            }

            public static void WriteBatch(IDisposable rowGroup, Type elementType, Array values, int count)
            {
                EnsureLoaded();

                IDisposable columnWriter = (IDisposable) rowGroup.GetType().GetMethod("NextColumn", Type.EmptyTypes).Invoke(rowGroup, null);
                try
                {
                    MethodInfo logicalWriterMethod = GetLogicalWriterMethod(columnWriter.GetType(), elementType);
                    IDisposable logicalWriter = (IDisposable) logicalWriterMethod.Invoke(columnWriter, new object[] { 4096 });
                    try
                    {
                        MethodInfo writeBatchMethod = GetWriteBatchMethod(logicalWriter.GetType(), values.GetType());
                        ParameterInfo[] parameters = writeBatchMethod.GetParameters();
                        if (parameters.Length == 1)
                            writeBatchMethod.Invoke(logicalWriter, new object[] { values });
                        else
                            writeBatchMethod.Invoke(logicalWriter, new object[] { values, 0, count });
                    }
                    finally
                    {
                        logicalWriter.Dispose();
                    }
                }
                finally
                {
                    columnWriter.Dispose();
                }
            }

            private static MethodInfo GetLogicalWriterMethod(Type columnWriterType, Type elementType)
            {
                lock (sync)
                {
                    MethodInfo method;
                    if (logicalWriterMethods.TryGetValue(elementType, out method))
                        return method;

                    MethodInfo[] methods = columnWriterType.GetMethods();
                    method = null;
                    for (int i = 0; i < methods.Length; i++)
                    {
                        MethodInfo candidate = methods[i];
                        if (!candidate.Name.Equals("LogicalWriter", StringComparison.Ordinal))
                            continue;
                        if (!candidate.IsGenericMethodDefinition)
                            continue;

                        ParameterInfo[] parameters = candidate.GetParameters();
                        if (parameters.Length != 1 || parameters[0].ParameterType != typeof(int))
                            continue;

                        method = candidate;
                        break;
                    }

                    if (method == null)
                        throw new InvalidOperationException("ParquetSharp ColumnWriter.LogicalWriter<T>() was not found.");

                    method = method.MakeGenericMethod(elementType);
                    logicalWriterMethods[elementType] = method;
                    return method;
                }
            }

            private static MethodInfo GetWriteBatchMethod(Type logicalWriterType, Type arrayType)
            {
                lock (sync)
                {
                    MethodInfo method;
                    if (writeBatchMethods.TryGetValue(arrayType, out method))
                        return method;

                    MethodInfo[] methods = logicalWriterType.GetMethods();
                    for (int i = 0; i < methods.Length; i++)
                    {
                        MethodInfo candidate = methods[i];
                        if (!candidate.Name.Equals("WriteBatch", StringComparison.Ordinal))
                            continue;

                        ParameterInfo[] parameters = candidate.GetParameters();
                        if (parameters.Length != 3)
                            continue;
                        if (!parameters[0].ParameterType.IsAssignableFrom(arrayType))
                            continue;

                        writeBatchMethods[arrayType] = candidate;
                        return candidate;
                    }

                    throw new InvalidOperationException("ParquetSharp LogicalWriter.WriteBatch(...) was not found for type " + arrayType.FullName);
                }
            }

            private static object TryGetCompressionValue()
            {
                if (compressionType == null)
                    return null;

                string[] preferredNames = { "Zstd", "ZSTD", "Snappy", "Gzip", "Uncompressed" };
                for (int i = 0; i < preferredNames.Length; i++)
                {
                    string name = preferredNames[i];
                    if (Enum.IsDefined(compressionType, name))
                        return Enum.Parse(compressionType, name);
                }

                Array values = Enum.GetValues(compressionType);
                return values.Length > 0 ? values.GetValue(0) : null;
            }

            private static void EnsureLoaded()
            {
                if (assembly != null)
                    return;

                lock (sync)
                {
                    if (assembly != null)
                        return;

                    assembly = ResolveAssembly();
                    columnBaseType = RequireType("ParquetSharp.Column");
                    columnGenericType = RequireType("ParquetSharp.Column`1");
                    writerType = RequireType("ParquetSharp.ParquetFileWriter");
                    compressionType = assembly.GetType("ParquetSharp.Compression", false);
                    appendRowGroupMethod = writerType.GetMethod("AppendRowGroup", Type.EmptyTypes);
                    closeWriterMethod = writerType.GetMethod("Close", Type.EmptyTypes);

                    if (appendRowGroupMethod == null || closeWriterMethod == null)
                        throw new InvalidOperationException("Unsupported ParquetSharp API. AppendRowGroup/Close methods were not found.");
                }
            }

            private static Assembly ResolveAssembly()
            {
                Assembly[] loadedAssemblies = AppDomain.CurrentDomain.GetAssemblies();
                for (int i = 0; i < loadedAssemblies.Length; i++)
                {
                    Assembly loaded = loadedAssemblies[i];
                    string name = loaded.GetName().Name;
                    if (string.Equals(name, "ParquetSharp", StringComparison.OrdinalIgnoreCase))
                        return loaded;
                }

                try
                {
                    return Assembly.Load("ParquetSharp");
                }
                catch
                {
                }

                string[] candidatePaths =
                {
                    Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "ParquetSharp.dll"),
                    Path.Combine(Globals.UserDataDir, "bin", "Custom", "ParquetSharp.dll"),
                    Path.Combine(Globals.UserDataDir, "bin", "Custom", "AddOns", "ParquetSharp.dll")
                };

                for (int i = 0; i < candidatePaths.Length; i++)
                {
                    string candidatePath = candidatePaths[i];
                    if (!File.Exists(candidatePath))
                        continue;

                    TryPreloadNativeDependency();
                    return Assembly.Load(File.ReadAllBytes(candidatePath));
                }

                throw new InvalidOperationException("ParquetSharp.dll was not found. Copy ParquetSharp.dll into NinjaTrader's custom bin folder before running the converter.");
            }

            private static void TryPreloadNativeDependency()
            {
                string[] candidatePaths =
                {
                    Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "ParquetSharpNative.dll"),
                    Path.Combine(Globals.UserDataDir, "bin", "Custom", "ParquetSharpNative.dll"),
                    Path.Combine(Globals.UserDataDir, "bin", "Custom", "AddOns", "ParquetSharpNative.dll")
                };

                for (int i = 0; i < candidatePaths.Length; i++)
                {
                    string candidatePath = candidatePaths[i];
                    if (!File.Exists(candidatePath))
                        continue;

                    if (LoadLibrary(candidatePath) != IntPtr.Zero)
                        return;
                }
            }

            private static Type RequireType(string typeName)
            {
                Type type = assembly.GetType(typeName, false);
                if (type == null)
                    throw new InvalidOperationException("ParquetSharp type was not found: " + typeName);
                return type;
            }

            private static object GetDefaultValue(Type type)
            {
                Type underlyingNullable = Nullable.GetUnderlyingType(type);
                if (underlyingNullable != null)
                    return null;
                if (!type.IsValueType)
                    return null;
                return Activator.CreateInstance(type);
            }

            [DllImport("kernel32", CharSet = CharSet.Unicode, SetLastError = true)]
            private static extern IntPtr LoadLibrary(string lpFileName);
        }

        private sealed class AtomicParquetWriter : IDisposable
        {
            public string TmpPath { get; private set; }
            public string FinalPath { get; private set; }
            public object Writer { get; private set; }

            public AtomicParquetWriter(string tmpPath, string finalPath, object writer)
            {
                TmpPath = tmpPath;
                FinalPath = finalPath;
                Writer = writer;
            }

            public void Dispose()
            {
                if (Writer != null)
                    ParquetDynamic.DisposeWriter(Writer);
            }
        }

        private sealed class FrameConversionResult
        {
            public FrameConversionResult(long lineCount, long badLineCount, long frameCount)
            {
                LineCount = lineCount;
                BadLineCount = badLineCount;
                FrameCount = frameCount;
            }

            public long LineCount { get; private set; }
            public long BadLineCount { get; private set; }
            public long FrameCount { get; private set; }
        }

        private sealed class L1Snapshot
        {
            public long BidPx;
            public int BidSz;
            public long AskPx;
            public int AskSz;
            public long LastPx;
            public int LastSz;
            public int DailyVolume;
        }

        private struct BestLevel
        {
            public BestLevel(long price, int size)
            {
                Price = price;
                Size = size;
            }

            public long Price { get; private set; }
            public int Size { get; private set; }
        }

        private sealed class OrderBook
        {
            private readonly long[] askPx;
            private readonly int[] askSz;
            private readonly long[] bidPx;
            private readonly int[] bidSz;

            public OrderBook(int depth)
            {
                Depth = depth;
                askPx = new long[depth];
                askSz = new int[depth];
                bidPx = new long[depth];
                bidSz = new int[depth];
            }

            public int Depth { get; private set; }
            public long[] AskPrices { get { return askPx; } }
            public int[] AskSizes { get { return askSz; } }
            public long[] BidPrices { get { return bidPx; } }
            public int[] BidSizes { get { return bidSz; } }

            public void SetLevel(int side, int position, long price, int size)
            {
                if (side == 0)
                {
                    askPx[position] = price;
                    askSz[position] = size;
                }
                else
                {
                    bidPx[position] = price;
                    bidSz[position] = size;
                }
            }

            public long GetPrice(int side, int position)
            {
                return side == 0 ? askPx[position] : bidPx[position];
            }

            public int GetSize(int side, int position)
            {
                return side == 0 ? askSz[position] : bidSz[position];
            }

            public BestLevel GetBestBid()
            {
                for (int i = 0; i < Depth; i++)
                {
                    if (bidSz[i] > 0 && bidPx[i] > 0)
                        return new BestLevel(bidPx[i], bidSz[i]);
                }

                return new BestLevel(0, 0);
            }

            public BestLevel GetBestAsk()
            {
                for (int i = 0; i < Depth; i++)
                {
                    if (askSz[i] > 0 && askPx[i] > 0)
                        return new BestLevel(askPx[i], askSz[i]);
                }

                return new BestLevel(0, 0);
            }

            public float ImbalanceTopN()
            {
                long bidTotal = 0;
                long askTotal = 0;

                for (int i = 0; i < Depth; i++)
                {
                    if (bidSz[i] > 0)
                        bidTotal += bidSz[i];
                    if (askSz[i] > 0)
                        askTotal += askSz[i];
                }

                long denominator = bidTotal + askTotal;
                if (denominator <= 0)
                    return 0f;

                return (float) (bidTotal - askTotal) / denominator;
            }
        }

        private sealed class FrameBuffer
        {
            public FrameBuffer(int depth, int capacity)
            {
                Depth = depth;
                Capacity = capacity;

                TsFrameNanos = new long[capacity];
                LastEventSeq = new long[capacity];
                HadEvent = new int[capacity];
                EventCount = new int[capacity];

                BestBidPx = new long[capacity];
                BestBidSz = new int[capacity];
                BestAskPx = new long[capacity];
                BestAskSz = new int[capacity];

                MidPx = new long[capacity];
                Spread = new long[capacity];
                ImbalanceTop1 = new float[capacity];
                ImbalanceTopN = new float[capacity];

                BidPx = new long[depth][];
                BidSz = new int[depth][];
                AskPx = new long[depth][];
                AskSz = new int[depth][];

                for (int i = 0; i < depth; i++)
                {
                    BidPx[i] = new long[capacity];
                    BidSz[i] = new int[capacity];
                    AskPx[i] = new long[capacity];
                    AskSz[i] = new int[capacity];
                }

                L1BidPx = new long[capacity];
                L1BidSz = new int[capacity];
                L1AskPx = new long[capacity];
                L1AskSz = new int[capacity];
                L1LastPx = new long[capacity];
                L1LastSz = new int[capacity];
                L1DailyVolume = new int[capacity];
            }

            public int Depth { get; private set; }
            public int Capacity { get; private set; }
            public int Count { get; private set; }
            public int RemainingCapacity { get { return Capacity - Count; } }

            public long[] TsFrameNanos { get; private set; }
            public long[] LastEventSeq { get; private set; }
            public int[] HadEvent { get; private set; }
            public int[] EventCount { get; private set; }

            public long[] BestBidPx { get; private set; }
            public int[] BestBidSz { get; private set; }
            public long[] BestAskPx { get; private set; }
            public int[] BestAskSz { get; private set; }

            public long[] MidPx { get; private set; }
            public long[] Spread { get; private set; }
            public float[] ImbalanceTop1 { get; private set; }
            public float[] ImbalanceTopN { get; private set; }

            public long[][] BidPx { get; private set; }
            public int[][] BidSz { get; private set; }
            public long[][] AskPx { get; private set; }
            public int[][] AskSz { get; private set; }

            public long[] L1BidPx { get; private set; }
            public int[] L1BidSz { get; private set; }
            public long[] L1AskPx { get; private set; }
            public int[] L1AskSz { get; private set; }
            public long[] L1LastPx { get; private set; }
            public int[] L1LastSz { get; private set; }
            public int[] L1DailyVolume { get; private set; }

            public void Append(long tsFrameNanos, long lastEventSeq, int hadEvent, int eventCount, long bestBidPx, int bestBidSz, long bestAskPx, int bestAskSz, long midPx, long spread, float imbalanceTop1, float imbalanceTopN, OrderBook book, L1Snapshot l1)
            {
                int index = Count;
                long[] bidPrices = book.BidPrices;
                int[] bidSizes = book.BidSizes;
                long[] askPrices = book.AskPrices;
                int[] askSizes = book.AskSizes;

                TsFrameNanos[index] = tsFrameNanos;
                LastEventSeq[index] = lastEventSeq;
                HadEvent[index] = hadEvent;
                EventCount[index] = eventCount;

                BestBidPx[index] = bestBidPx;
                BestBidSz[index] = bestBidSz;
                BestAskPx[index] = bestAskPx;
                BestAskSz[index] = bestAskSz;

                MidPx[index] = midPx;
                Spread[index] = spread;
                ImbalanceTop1[index] = imbalanceTop1;
                ImbalanceTopN[index] = imbalanceTopN;

                for (int level = 0; level < Depth; level++)
                {
                    BidPx[level][index] = bidPrices[level];
                    BidSz[level][index] = bidSizes[level];
                    AskPx[level][index] = askPrices[level];
                    AskSz[level][index] = askSizes[level];
                }

                L1BidPx[index] = l1.BidPx;
                L1BidSz[index] = l1.BidSz;
                L1AskPx[index] = l1.AskPx;
                L1AskSz[index] = l1.AskSz;
                L1LastPx[index] = l1.LastPx;
                L1LastSz[index] = l1.LastSz;
                L1DailyVolume[index] = l1.DailyVolume;

                Count++;
            }

            public void AppendRepeated(long startBucket, long frameNs, int frameCount, FrameSnapshot snapshot)
            {
                int startIndex = Count;
                int endIndex = startIndex + frameCount;
                long tsFrameNanos = checked((startBucket + 1) * frameNs);

                for (int index = startIndex; index < endIndex; index++)
                {
                    TsFrameNanos[index] = tsFrameNanos;
                    tsFrameNanos += frameNs;
                }

                FillLong(LastEventSeq, startIndex, frameCount, 0);
                FillInt(HadEvent, startIndex, frameCount, 0);
                FillInt(EventCount, startIndex, frameCount, 0);

                FillLong(BestBidPx, startIndex, frameCount, snapshot.BestBidPx);
                FillInt(BestBidSz, startIndex, frameCount, snapshot.BestBidSz);
                FillLong(BestAskPx, startIndex, frameCount, snapshot.BestAskPx);
                FillInt(BestAskSz, startIndex, frameCount, snapshot.BestAskSz);
                FillLong(MidPx, startIndex, frameCount, snapshot.MidPx);
                FillLong(Spread, startIndex, frameCount, snapshot.Spread);
                FillFloat(ImbalanceTop1, startIndex, frameCount, snapshot.ImbalanceTop1);
                FillFloat(ImbalanceTopN, startIndex, frameCount, snapshot.ImbalanceTopN);

                for (int level = 0; level < Depth; level++)
                {
                    FillLong(BidPx[level], startIndex, frameCount, snapshot.BidPx[level]);
                    FillInt(BidSz[level], startIndex, frameCount, snapshot.BidSz[level]);
                    FillLong(AskPx[level], startIndex, frameCount, snapshot.AskPx[level]);
                    FillInt(AskSz[level], startIndex, frameCount, snapshot.AskSz[level]);
                }

                FillLong(L1BidPx, startIndex, frameCount, snapshot.L1BidPx);
                FillInt(L1BidSz, startIndex, frameCount, snapshot.L1BidSz);
                FillLong(L1AskPx, startIndex, frameCount, snapshot.L1AskPx);
                FillInt(L1AskSz, startIndex, frameCount, snapshot.L1AskSz);
                FillLong(L1LastPx, startIndex, frameCount, snapshot.L1LastPx);
                FillInt(L1LastSz, startIndex, frameCount, snapshot.L1LastSz);
                FillInt(L1DailyVolume, startIndex, frameCount, snapshot.L1DailyVolume);

                Count = endIndex;
            }

            public void Reset()
            {
                Count = 0;
            }

            private static void FillLong(long[] values, int start, int count, long value)
            {
                int end = start + count;
                for (int i = start; i < end; i++)
                    values[i] = value;
            }

            private static void FillInt(int[] values, int start, int count, int value)
            {
                int end = start + count;
                for (int i = start; i < end; i++)
                    values[i] = value;
            }

            private static void FillFloat(float[] values, int start, int count, float value)
            {
                int end = start + count;
                for (int i = start; i < end; i++)
                    values[i] = value;
            }
        }

        private sealed class FrameSnapshot
        {
            public FrameSnapshot(int depth)
            {
                Depth = depth;
                BidPx = new long[depth];
                BidSz = new int[depth];
                AskPx = new long[depth];
                AskSz = new int[depth];
            }

            public int Depth { get; private set; }
            public long BestBidPx;
            public int BestBidSz;
            public long BestAskPx;
            public int BestAskSz;
            public long MidPx;
            public long Spread;
            public float ImbalanceTop1;
            public float ImbalanceTopN;
            public long[] BidPx;
            public int[] BidSz;
            public long[] AskPx;
            public int[] AskSz;
            public long L1BidPx;
            public int L1BidSz;
            public long L1AskPx;
            public int L1AskSz;
            public long L1LastPx;
            public int L1LastSz;
            public int L1DailyVolume;
        }

        private struct ParsedEvent
        {
            public int Kind;
            public long TimestampNanos;
            public long Sequence;
            public int Type;
            public int Operation;
            public int Position;
            public long PriceI64;
            public int Volume;

            public static ParsedEvent CreateL1(long timestampNanos, long sequence, int type, long priceI64, int volume)
            {
                ParsedEvent parsedEvent = new ParsedEvent();
                parsedEvent.Kind = 1;
                parsedEvent.TimestampNanos = timestampNanos;
                parsedEvent.Sequence = sequence;
                parsedEvent.Type = type;
                parsedEvent.PriceI64 = priceI64;
                parsedEvent.Volume = volume;
                return parsedEvent;
            }

            public static ParsedEvent CreateL2(long timestampNanos, long sequence, int type, int operation, int position, long priceI64, int volume)
            {
                ParsedEvent parsedEvent = new ParsedEvent();
                parsedEvent.Kind = 2;
                parsedEvent.TimestampNanos = timestampNanos;
                parsedEvent.Sequence = sequence;
                parsedEvent.Type = type;
                parsedEvent.Operation = operation;
                parsedEvent.Position = position;
                parsedEvent.PriceI64 = priceI64;
                parsedEvent.Volume = volume;
                return parsedEvent;
            }
        }
    }

    public class DumpEntry
    {
        public long NrdLength { get; set; }
        public Instrument Instrument { get; set; }
        public DateTime Date { get; set; }
        public string OutputPartitionDir { get; set; }
        public string FromName { get; set; }
        public string ToName { get; set; }
    }
}
