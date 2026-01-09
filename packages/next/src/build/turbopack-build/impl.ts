// Import cpu-profile first to start profiling early if enabled
import { saveCpuProfile } from '../../server/lib/cpu-profile'
import path from 'path'
import { validateTurboNextConfig } from '../../lib/turbopack-warning'
import { isFileSystemCacheEnabledForBuild } from '../../shared/lib/turbopack/utils'
import { NextBuildContext } from '../build-context'
import { createDefineEnv, getBindingsSync } from '../swc'
import { installBindings } from '../swc/install-bindings'
import {
  handleRouteType,
  rawEntrypointsToEntrypoints,
} from '../handle-entrypoints'
import { TurbopackManifestLoader } from '../../shared/lib/turbopack/manifest-loader'
import { promises as fs } from 'fs'
import { PHASE_PRODUCTION_BUILD } from '../../shared/lib/constants'
import loadConfig from '../../server/config'
import { hasCustomExportOutput } from '../../export/utils'
import { Telemetry } from '../../telemetry/storage'
import { setGlobal } from '../../trace'
import { isCI } from '../../server/ci-info'
import { backgroundLogCompilationEvents } from '../../shared/lib/turbopack/compilation-events'
import { getSupportedBrowsers, printBuildErrors } from '../utils'
import { normalizePath } from '../../lib/normalize-path'
import type { RawEntrypoints, TurbopackResult } from '../swc/types'

export async function turbopackBuild(): Promise<{
  duration: number
  buildTraceContext: undefined
  shutdownPromise: Promise<void>
}> {
  await validateTurboNextConfig({
    dir: NextBuildContext.dir!,
    configPhase: PHASE_PRODUCTION_BUILD,
  })

  const config = NextBuildContext.config!
  const dir = NextBuildContext.dir!
  const distDir = NextBuildContext.distDir!
  const buildId = NextBuildContext.buildId!
  const encryptionKey = NextBuildContext.encryptionKey!
  const previewProps = NextBuildContext.previewProps!
  const hasRewrites = NextBuildContext.hasRewrites!
  const rewrites = NextBuildContext.rewrites!
  const noMangling = NextBuildContext.noMangling!
  const currentNodeJsVersion = process.versions.node

  const startTime = process.hrtime()
  const bindings = getBindingsSync() // our caller should have already loaded these
  const dev = false

  const supportedBrowsers = getSupportedBrowsers(dir, dev)

  const persistentCaching = isFileSystemCacheEnabledForBuild(config)
  const rootPath = config.turbopack?.root || config.outputFileTracingRoot || dir
  const project = await bindings.turbo.createProject(
    {
      rootPath: config.turbopack?.root || config.outputFileTracingRoot || dir,
      projectPath: normalizePath(path.relative(rootPath, dir) || '.'),
      distDir,
      nextConfig: config,
      watch: {
        enable: false,
      },
      dev,
      env: process.env as Record<string, string>,
      defineEnv: createDefineEnv({
        isTurbopack: true,
        clientRouterFilters: NextBuildContext.clientRouterFilters!,
        config,
        dev,
        distDir,
        projectPath: dir,
        fetchCacheKeyPrefix: config.experimental.fetchCacheKeyPrefix,
        hasRewrites,
        // Implemented separately in Turbopack, doesn't have to be passed here.
        middlewareMatchers: undefined,
        rewrites,
      }),
      buildId,
      encryptionKey,
      previewProps,
      browserslistQuery: supportedBrowsers.join(', '),
      noMangling,
      writeRoutesHashesManifest:
        !!process.env.NEXT_TURBOPACK_WRITE_ROUTES_HASHES_MANIFEST,
      currentNodeJsVersion,
    },
    {
      persistentCaching,
      memoryLimit: config.experimental?.turbopackMemoryLimit,
      dependencyTracking: persistentCaching,
      isCi: isCI,
      isShortSession: true,
    }
  )
  try {
    backgroundLogCompilationEvents(project)

    // Write an empty file in a known location to signal this was built with Turbopack
    await fs.writeFile(path.join(distDir, 'turbopack'), '')

    await fs.mkdir(path.join(distDir, 'server'), { recursive: true })
    await fs.mkdir(path.join(distDir, 'static', buildId), {
      recursive: true,
    })
    await fs.writeFile(
      path.join(distDir, 'package.json'),
      '{"type": "commonjs"}'
    )

    let appDirOnly = NextBuildContext.appDirOnly!

    // Check if deferred entries are configured
    const hasDeferredEntries =
      config.experimental.deferredEntries &&
      config.experimental.deferredEntries.length > 0

    let entrypoints: TurbopackResult<Partial<RawEntrypoints>>

    if (hasDeferredEntries) {
      // Phase 1: Write non-deferred entrypoints first
      const nonDeferredEntrypoints =
        await project.writeNonDeferredEntrypointsToDisk(appDirOnly)
      printBuildErrors(nonDeferredEntrypoints, dev)

      // Call onBeforeDeferredEntries callback before compiling deferred entries
      if (config.experimental.onBeforeDeferredEntries) {
        await config.experimental.onBeforeDeferredEntries()
      }

      // Phase 2: Write deferred entrypoints
      const deferredEntrypoints =
        await project.writeDeferredEntrypointsToDisk(appDirOnly)
      printBuildErrors(deferredEntrypoints, dev)

      // Merge the entrypoints from both phases
      // Use the non-deferred result as base since it has middleware/instrumentation
      entrypoints = {
        ...nonDeferredEntrypoints,
        routes: new Map([
          ...(nonDeferredEntrypoints.routes || []),
          ...(deferredEntrypoints.routes || []),
        ]),
      }
    } else {
      // No deferred entries - use the standard single-phase build
      entrypoints = await project.writeAllEntrypointsToDisk(appDirOnly)
      printBuildErrors(entrypoints, dev)
    }

    let routes = entrypoints.routes
    if (!routes) {
      // This should never ever happen, there should be an error issue, or the bindings call should
      // have thrown.
      throw new Error(`Turbopack build failed`)
    }

    const hasPagesEntries = Array.from(routes.values()).some((route) => {
      if (route.type === 'page' || route.type === 'page-api') {
        return true
      }
      return false
    })
    // If there's no pages entries, then we are in app-dir-only mode
    if (!hasPagesEntries) {
      appDirOnly = true
    }

    const manifestLoader = new TurbopackManifestLoader({
      buildId,
      distDir,
      encryptionKey,
    })

    const currentEntrypoints = await rawEntrypointsToEntrypoints(
      entrypoints as TurbopackResult<RawEntrypoints>
    )

    // Process all routes
    const routePromises: Promise<void>[] = []

    if (!appDirOnly) {
      for (const [page, route] of currentEntrypoints.page) {
        routePromises.push(
          handleRouteType({
            page,
            route,
            manifestLoader,
          })
        )
      }
    }

    for (const [page, route] of currentEntrypoints.app) {
      routePromises.push(
        handleRouteType({
          page,
          route,
          manifestLoader,
        })
      )
    }

    await Promise.all(routePromises)

    await Promise.all([
      // Only load pages router manifests if not app-only
      ...(!appDirOnly
        ? [
            manifestLoader.loadBuildManifest('_app'),
            manifestLoader.loadPagesManifest('_app'),
            manifestLoader.loadFontManifest('_app'),
            manifestLoader.loadPagesManifest('_document'),
            manifestLoader.loadClientBuildManifest('_error'),
            manifestLoader.loadBuildManifest('_error'),
            manifestLoader.loadPagesManifest('_error'),
            manifestLoader.loadFontManifest('_error'),
          ]
        : []),
      entrypoints.instrumentation &&
        manifestLoader.loadMiddlewareManifest(
          'instrumentation',
          'instrumentation'
        ),
      entrypoints.middleware &&
        (await manifestLoader.loadMiddlewareManifest(
          'middleware',
          'middleware'
        )),
    ])

    manifestLoader.writeManifests({
      devRewrites: undefined,
      productionRewrites: rewrites,
      entrypoints: currentEntrypoints,
    })

    if (NextBuildContext.analyze) {
      await project.writeAnalyzeData(appDirOnly)
    }

    const shutdownPromise = project.shutdown()

    const time = process.hrtime(startTime)
    return {
      duration: time[0] + time[1] / 1e9,
      buildTraceContext: undefined,
      shutdownPromise,
    }
  } catch (err) {
    await project.shutdown()
    throw err
  }
}

let shutdownPromise: Promise<void> | undefined
export async function workerMain(workerData: {
  buildContext: typeof NextBuildContext
}): Promise<
  Omit<Awaited<ReturnType<typeof turbopackBuild>>, 'shutdownPromise'>
> {
  // setup new build context from the serialized data passed from the parent
  Object.assign(NextBuildContext, workerData.buildContext)

  /// load the config because it's not serializable
  const config = (NextBuildContext.config = await loadConfig(
    PHASE_PRODUCTION_BUILD,
    NextBuildContext.dir!,
    {
      debugPrerender: NextBuildContext.debugPrerender,
      reactProductionProfiling: NextBuildContext.reactProductionProfiling,
    }
  ))
  // Matches handling in build/index.ts
  // https://github.com/vercel/next.js/blob/84f347fc86f4efc4ec9f13615c215e4b9fb6f8f0/packages/next/src/build/index.ts#L815-L818
  // Ensures the `config.distDir` option is matched.
  if (hasCustomExportOutput(NextBuildContext.config)) {
    NextBuildContext.config.distDir = '.next'
  }

  // Clone the telemetry for worker
  const telemetry = new Telemetry({
    distDir: NextBuildContext.config.distDir,
  })
  setGlobal('telemetry', telemetry)
  // Install bindings early so we can access synchronously later
  await installBindings(config.experimental?.useWasmBinary)

  try {
    const {
      shutdownPromise: resultShutdownPromise,
      buildTraceContext,
      duration,
    } = await turbopackBuild()
    shutdownPromise = resultShutdownPromise
    return {
      buildTraceContext,
      duration,
    }
  } finally {
    // Always flush telemetry before worker exits (waits for async operations like setTimeout in debug mode)
    await telemetry.flush()
    // Save CPU profile before worker exits
    await saveCpuProfile()
  }
}

export async function waitForShutdown(): Promise<void> {
  if (shutdownPromise) {
    await shutdownPromise
  }
}
