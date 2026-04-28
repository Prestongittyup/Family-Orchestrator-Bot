const FALLBACK_API_BASE_URL = "/api";

const normalizeApiBase = (value: string | undefined): string => {
  const trimmed = value?.trim();
  if (!trimmed) {
    return FALLBACK_API_BASE_URL;
  }
  return trimmed.replace(/\/$/, "");
};

export const API_BASE_URL = normalizeApiBase(import.meta.env.VITE_API_BASE_URL as string | undefined);

const isAbsoluteUrl = (value: string): boolean => /^https?:\/\//i.test(value);

export const isNetworkError = (error: unknown): error is TypeError =>
  error instanceof TypeError && /fetch|network/i.test(error.message);

export const buildApiUrl = (path: string, baseUrl: string = API_BASE_URL): string => {
  if (isAbsoluteUrl(path)) {
    return path;
  }
  const normalizedPath = path.startsWith("/") ? path : `/${path}`;
  return `${baseUrl}${normalizedPath}`;
};

const withNetworkContext = (error: unknown, url: string): Error => {
  const message = error instanceof Error ? error.message : String(error);
  return new Error(`Network error calling ${url}: ${message}`);
};

export const fetchWithApiFallback = async (path: string, init: RequestInit): Promise<Response> => {
  const primaryUrl = buildApiUrl(path);
  try {
    return await fetch(primaryUrl, init);
  } catch (error) {
    if (!isNetworkError(error)) {
      throw error;
    }

    if (isAbsoluteUrl(path) || API_BASE_URL === FALLBACK_API_BASE_URL) {
      throw withNetworkContext(error, primaryUrl);
    }

    const fallbackUrl = buildApiUrl(path, FALLBACK_API_BASE_URL);
    try {
      return await fetch(fallbackUrl, init);
    } catch (fallbackError) {
      const message = fallbackError instanceof Error ? fallbackError.message : String(fallbackError);
      throw new Error(
        `Network error calling ${primaryUrl} (fallback ${fallbackUrl} also failed): ${message}`
      );
    }
  }
};
