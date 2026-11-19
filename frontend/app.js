const { invoke } = window.__TAURI__.core;
const { listen } = window.__TAURI__.event;
const { getCurrentWindow } = window.__TAURI__.window;

const appWindow = getCurrentWindow();
document.getElementById('btn-min').onclick = () => appWindow.minimize();
document.getElementById('btn-close').onclick = () => appWindow.close();

const searchMode = document.getElementById('search-mode');
const riverMode = document.getElementById('river-mode');
const searchInput = document.getElementById('search-input');
const btnSearch = document.getElementById('btn-search');
const searchResults = document.getElementById('search-results');
const trackTitle = document.getElementById('track-title');
const trackArtist = document.getElementById('track-artist');
const btnPlayPause = document.getElementById('btn-play-pause');
const btnStopNow = document.getElementById('btn-stop-now');
const volumeSlider = document.getElementById('volume-slider');
const decisionBar = document.getElementById('decision-bar');

function showSearchMode() {
  riverMode.classList.add('hidden');
  decisionBar.classList.add('hidden');
  searchMode.classList.remove('hidden');
  searchInput.value = '';
  searchResults.innerHTML = '';
  searchInput.focus();
}

function showRiverMode() {
  searchMode.classList.add('hidden');
  decisionBar.classList.add('hidden');
  riverMode.classList.remove('hidden');
  btnPlayPause.textContent = '⏸';
}

async function runSearch() {
  const q = searchInput.value.trim();
  if (!q) {
    searchResults.innerHTML = '';
    return;
  }
  searchResults.innerHTML = '<li>검색 중...</li>';
  try {
    const tracks = await invoke('search_tracks', { query: q });
    searchResults.innerHTML = '';
    if (!tracks || tracks.length === 0) {
      const li = document.createElement('li');
      li.textContent = '검색 결과가 없습니다.';
      searchResults.appendChild(li);
      return;
    }
    tracks.slice(0, 5).forEach(t => {
      const li = document.createElement('li');
      li.textContent = `${t.title} - ${t.artist}`;
      li.onclick = async () => {
        showRiverMode();
        trackTitle.textContent = t.title;
        trackArtist.textContent = `${t.artist} (불러오는 중...)`;
        try {
          await invoke('start_river', { track: t });
        } catch (e) {
          trackArtist.textContent = `재생 오류: ${e}`;
        }
      };
      searchResults.appendChild(li);
    });
  } catch (err) {
    searchResults.innerHTML = '';
    const li = document.createElement('li');
    li.textContent = `오류: ${err}`;
    searchResults.appendChild(li);
  }
}

btnSearch.onclick = runSearch;
searchInput.addEventListener('keydown', (e) => {
  if (e.key === 'Enter') runSearch();
});

btnPlayPause.onclick = async () => {
  try {
    const isPaused = await invoke('toggle_pause');
    btnPlayPause.textContent = isPaused ? '▶' : '⏸';
  } catch (_) {}
};

btnStopNow.onclick = async () => {
  await invoke('stop_river');
  showSearchMode();
};

volumeSlider.addEventListener('input', async (e) => {
  const vol = parseFloat(e.target.value);
  await invoke('set_volume', { volume: vol });
});

document.getElementById('btn-continue').onclick = async () => {
  decisionBar.classList.add('hidden');
  btnPlayPause.textContent = '⏸';
  await invoke('continue_river');
};

document.getElementById('btn-stop').onclick = async () => {
  await invoke('stop_river');
  showSearchMode();
};

listen('river-track-changed', ({ payload }) => {
  trackTitle.textContent = payload.title;
  trackArtist.textContent = payload.artist;
  btnPlayPause.textContent = '⏸';
});

listen('river-waiting', () => {
  decisionBar.classList.remove('hidden');
});
