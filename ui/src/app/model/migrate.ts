import { IDataflowConfig, IDataprocConfig } from "./profile";
import ITargetDetails from "./target-details";

export default interface IMigrationDetails {
    TargetDetails: ITargetDetails
    DataflowConfig: IDataflowConfig
    DataprocConfig: IDataprocConfig
    MigrationType: string
    MigrationMode: string
}

export interface IProgress {
    Progress: number
    ErrorMessage: string
    ProgressStatus: number
}

// TODO: eenclona@ will probably need to update this with dataproc specific generated resources
export interface IGeneratedResources {
    DatabaseName: string
    DatabaseUrl: string
    BucketName: string
    BucketUrl: string
    DataStreamJobName: string
    DataStreamJobUrl: string
    DataflowJobName: string
    DataflowJobUrl: string
}

export interface ISourceAndTargetDetails {
    SpannerDatabaseName: string
    SpannerDatabaseUrl: string
    SourceDatabaseName: string
    SourceDatabaseType: string
}