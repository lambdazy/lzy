import overview from '../docs/0-overview.md';
import setup from '../docs/1-setup.md';
import basics from '../docs/2-basics.md';
import data from '../docs/3-data.md';
import environment from '../docs/4-environment.md';
import whiteboard from '../docs/5-whiteboards.md';
import views from '../docs/6-views.md';
import integrations from '../docs/7-integrations.md'
import ReactMarkdown from 'react-markdown'
import {useState} from 'react';

export function Overview() {
    let [state, setState] = useState<any>(null);
    if (state == null) {
        fetch(overview)
            .then(async (response) => {
                    let mdtext = await response.text();
                    setState(<ReactMarkdown className="markdown-body">{mdtext}</ReactMarkdown>);
                }
            )
    }
    return (<>{state}</>)
}

export function Setup() {
    let [state, setState] = useState<any>(null);
    if (state == null) {
        fetch(setup)
            .then(async (response) => {
                    let mdtext = await response.text();
                    setState(<ReactMarkdown className="markdown-body">{mdtext}</ReactMarkdown>);
                }
            )
    }
    return (
        <>
            {state}
        </>
    )
}

export function Basics() {
    let [state, setState] = useState<any>(null);
    if (state == null) {
        fetch(basics)
            .then(async (response) => {
                    let mdtext = await response.text();
                    setState(<ReactMarkdown className="markdown-body">{mdtext}</ReactMarkdown>);
                }
            )
    }
    return (
        <>
            {state}
        </>
    )
}

export function Environment() {
    let [state, setState] = useState<any>(null);
    if (state == null) {
        fetch(environment)
            .then(async (response) => {
                    let mdtext = await response.text();
                    setState(<ReactMarkdown className="markdown-body">{mdtext}</ReactMarkdown>);
                }
            )
    }
    return (
        <>
            {state}
        </>
    )
}

export function Data() {
    let [state, setState] = useState<any>(null);
    if (state == null) {
        fetch(data)
            .then(async (response) => {
                    let mdtext = await response.text();
                    setState(<ReactMarkdown className="markdown-body">{mdtext}</ReactMarkdown>);
                }
            )
    }
    return (
        <>
            {state}
        </>
    )
}

export function Whiteboards() {
    let [state, setState] = useState<any>(null);
    if (state == null) {
        fetch(whiteboard)
            .then(async (response) => {
                    let mdtext = await response.text();
                    setState(<ReactMarkdown className="markdown-body">{mdtext}</ReactMarkdown>);
                }
            )
    }
    return (
        <>
            {state}
        </>
    )
}

export function Views() {
    let [state, setState] = useState<any>(null);
    if (state == null) {
        fetch(views)
            .then(async (response) => {
                    let mdtext = await response.text();
                    setState(<ReactMarkdown className="markdown-body">{mdtext}</ReactMarkdown>);
                }
            )
    }
    return (
        <>
            {state}
        </>
    )
}

export function Integrations() {
    let [state, setState] = useState<any>(null);
    if (state == null) {
        fetch(integrations)
            .then(async (response) => {
                    let mdtext = await response.text();
                    setState(<ReactMarkdown className="markdown-body">{mdtext}</ReactMarkdown>);
                }
            )
    }
    return (
        <>
            {state}
        </>
    )
}
