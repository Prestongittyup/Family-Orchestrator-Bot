import { describe, expect, it } from "vitest";
import { readdirSync, readFileSync } from "node:fs";
import { join, resolve } from "node:path";

const FRONTEND_ROOT = resolve(__dirname, "..");
const CONTRACT_FILE = resolve(FRONTEND_ROOT, "src/ui/contracts/homeUxSurfaceContract.ts");
const DASHBOARD_FILE = resolve(FRONTEND_ROOT, "src/ui/screens/DashboardScreen.tsx");

const collectFiles = (dirPath: string, extensions: string[]): string[] => {
  const files: string[] = [];
  for (const entry of readdirSync(dirPath, { withFileTypes: true })) {
    const fullPath = join(dirPath, entry.name);
    if (entry.isDirectory()) {
      files.push(...collectFiles(fullPath, extensions));
      continue;
    }
    if (extensions.some((ext) => entry.name.endsWith(ext))) {
      files.push(fullPath);
    }
  }
  return files;
};

const relativePath = (filePath: string): string => {
  return filePath.replace(FRONTEND_ROOT + "\\", "").replace(/\\/g, "/");
};

describe("home ux surface contract enforcement", () => {
  it("keeps home priority semantics out of screen-level local logic", () => {
    const homeRelatedScreens = collectFiles(resolve(FRONTEND_ROOT, "src/ui/screens"), [".ts", ".tsx"])
      .filter((filePath) => {
        const source = readFileSync(filePath, "utf-8");
        return source.includes("fetchHomeV0(") || source.includes("dashboard-home-focus") || source.includes("Home Focus");
      });

    const forbiddenPatterns: RegExp[] = [
      /labelAttentionTier\s*=\s*\(/,
      /labelActionability\s*=\s*\(/,
      /\bcritical_now\b|\bnext_up\b|\blater\b/,
      /\bdecide_now\b|\bdo_next\b|\bstay_informed\b/,
      /NEXT_UP_VISIBLE_LIMIT|LATER_VISIBLE_LIMIT/,
    ];

    const offenders = homeRelatedScreens.flatMap((filePath) => {
      if (filePath === CONTRACT_FILE) {
        return [];
      }

      const source = readFileSync(filePath, "utf-8");
      if (forbiddenPatterns.some((pattern) => pattern.test(source))) {
        return [relativePath(filePath)];
      }
      return [];
    });

    expect(offenders).toEqual([]);
  });

  it("routes Home UX through a single contract entry point", () => {
    const dashboardSource = readFileSync(DASHBOARD_FILE, "utf-8");

    expect(dashboardSource).toContain('from "../contracts/homeUxSurfaceContract"');
    expect(dashboardSource).toContain("interpretHomeUxSurfaceContract(homeContract)");
    expect(dashboardSource).not.toContain("homePriorityModel");
  });
});
